#include "wal.h"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <system_error>

namespace oxigraph {

// CRC32C implementation (Castagnoli) using standard polynomial 0x1EDC6F41
uint32_t WAL::compute_crc32c(const uint8_t* data, size_t length) noexcept {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0x82F63B78; // Reversed polynomial for 0x1EDC6F41
            } else {
                crc >>= 1;
            }
        }
    }
    return ~crc;
}

WAL::WAL(const std::string& log_file_path)
    : file_path_(log_file_path),
      fd_(-1),
      next_lsn_(1),
      committed_lsn_(0),
      flushed_lsn_(0),
      stop_writer_(false),
      bytes_written_in_buffer_(0) {
    
    // Open the WAL file with O_RDWR | O_CREAT | O_APPEND, standard POSIX
    fd_ = ::open(file_path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to open WAL file: " + file_path_);
    }

    // Determine current file size and latest LSN if recovering
    struct stat st;
    if (::fstat(fd_, &st) == 0) {
        if (st.st_size > 0) {
            // Read the end of the file or scan to find the last valid LSN if necessary.
            // For initialization, we scan existing records to set next_lsn_ properly.
            recover_last_lsn();
        }
    }

    // Pre-allocate buffer
    write_buffer_.resize(DEFAULT_BUFFER_SIZE);

    // Start background flush worker thread
    writer_thread_ = std::thread(&WAL::background_writer_loop, this);
}

WAL::~WAL() {
    try {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_writer_ = true;
        }
        cv_.notify_all();
        if (writer_thread_.joinable()) {
            writer_thread_.join();
        }

        // Final flush of remaining buffer
        flush_buffer_locked();

        if (fd_ >= 0) {
            ::fsync(fd_);
            ::close(fd_);
            fd_ = -1;
        }
    } catch (...) {
        // Suppress exceptions in destructor
    }
}

LSN WAL::append(uint64_t tx_id, RecordType type, const uint8_t* payload, uint32_t payload_len) {
    std::lock_guard<std::mutex> lock(mutex_);

    LSN assigned_lsn = next_lsn_++;

    // Calculate total record size: 
    // [LSN (8) | TxID (8) | Type (1) | CRC32C (4) | PayloadLen (4) | Payload (...)]
    size_t header_size = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
    size_t total_record_size = header_size + payload_len;

    // Check if buffer needs to be flushed to accommodate the new record
    if (bytes_written_in_buffer_ + total_record_size > write_buffer_.size()) {
        flush_buffer_locked();
        if (total_record_size > write_buffer_.size()) {
            // Resize buffer dynamically if a single record exceeds default buffer size
            write_buffer_.resize(total_record_size + 4096);
        }
    }

    // Prepare temporary serialization buffer for CRC32C computation
    std::vector<uint8_t> temp_payload_len_prefix(sizeof(uint32_t));
    std::memcpy(temp_payload_len_prefix.data(), &payload_len, sizeof(uint32_t));

    // Compute CRC32C over: [TxID | Type | PayloadLen | Payload]
    // As per spec: [LSN (8B) | TxID (8B) | Type (1B) | CRC32C (4B) | Payload Length (4B) | Payload (...) ]
    // CRC is typically computed over the record contents excluding LSN and CRC itself.
    
    // We compute CRC over TxID + Type + Payload Length + Payload
    std::vector<uint8_t> crc_data;
    crc_data.reserve(sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) + payload_len);
    
    uint64_t net_tx_id = tx_id;
    uint8_t net_type = static_cast<uint8_t>(type);
    
    crc_data.insert(crc_data.end(), reinterpret_cast<uint8_t*>(&net_tx_id), reinterpret_cast<uint8_t*>(&net_tx_id) + sizeof(uint64_t));
    crc_data.push_back(net_type);
    crc_data.insert(crc_data.end(), reinterpret_cast<uint8_t*>(&payload_len), reinterpret_cast<uint8_t*>(&payload_len) + sizeof(uint32_t));
    if (payload_len > 0 && payload != nullptr) {
        crc_data.insert(crc_data.end(), payload, payload + payload_len);
    }

    uint32_t crc = compute_crc32c(crc_data.data(), crc_data.size());

    // Write into memory buffer
    uint8_t* dest = write_buffer_.data() + bytes_written_in_buffer_;
    
    std::memcpy(dest, &assigned_lsn, sizeof(uint64_t));
    dest += sizeof(uint64_t);

    std::memcpy(dest, &net_tx_id, sizeof(uint64_t));
    dest += sizeof(uint64_t);

    *dest = net_type;
    dest += sizeof(uint8_t);

    std::memcpy(dest, &crc, sizeof(uint32_t));
    dest += sizeof(uint32_t);

    std::memcpy(dest, &payload_len, sizeof(uint32_t));
    dest += sizeof(uint32_t);

    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(dest, payload, payload_len);
    }

    bytes_written_in_buffer_ += total_record_size;

    // Notify background worker if buffer is filling up or flush is requested
    if (bytes_written_in_buffer_ >= write_buffer_.size() / 2 || type == RecordType::COMMIT || type == RecordType::CHECKPOINT) {
        cv_.notify_one();
    }

    return assigned_lsn;
}

void WAL::flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    flush_buffer_locked();
    LSN target_lsn = next_lsn_ - 1;
    
    // Wait until background thread or synchronous write flushes up to target_lsn
    cv_.wait(lock, [this, target_lsn] {
        return flushed_lsn_ >= target_lsn;
    });
}

void WAL::flush_to(LSN lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (lsn <= flushed_lsn_) {
        return;
    }
    flush_buffer_locked();
    
    cv_.wait(lock, [this, lsn] {
        return flushed_lsn_ >= lsn;
    });
}

LSN WAL::get_flushed_lsn() const noexcept {
    return flushed_lsn_.load(std::memory_order_acquire);
}

void WAL::checkpoint() {
    // Append a checkpoint record and flush synchronously
    uint64_t dummy_tx = 0;
    LSN lsn = append(dummy_tx, RecordType::CHECKPOINT, nullptr, 0);
    flush_to(lsn);
}

void WAL::recover(std::function<void(LSN, uint64_t, RecordType, const uint8_t*, uint32_t)> apply_callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Ensure any buffered data is written to disk first
    flush_buffer_locked();

    // Seek to beginning of WAL file
    if (::lseek(fd_, 0, SEEK_SET) < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to seek WAL file for recovery");
    }

    while (true) {
        uint64_t lsn = 0;
        uint64_t tx_id = 0;
        uint8_t type_val = 0;
        uint32_t stored_crc = 0;
        uint32_t payload_len = 0;

        ssize_t n = ::read(fd_, &lsn, sizeof(uint64_t));
        if (n == 0) {
            break; // End of file
        }
        if (n < 0 || static_cast<size_t>(n) < sizeof(uint64_t)) {
            break; // Corrupted or incomplete header, stop recovery
        }

        if (::read(fd_, &tx_id, sizeof(uint64_t)) != sizeof(uint64_t)) break;
        if (::read(fd_, &type_val, sizeof(uint8_t)) != sizeof(uint8_t)) break;
        if (::read(fd_, &stored_crc, sizeof(uint32_t)) != sizeof(uint32_t)) break;
        if (::read(fd_, &payload_len, sizeof(uint32_t)) != sizeof(uint32_t)) break;

        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            if (::read(fd_, payload.data(), payload_len) != static_cast<ssize_t>(payload_len)) {
                break; // Incomplete payload
            }
        }

        // Validate CRC32C
        std::vector<uint8_t> crc_data;
        crc_data.reserve(sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) + payload_len);
        
        uint64_t net_tx_id = tx_id;
        uint8_t net_type = type_val;
        
        crc_data.insert(crc_data.end(), reinterpret_cast<uint8_t*>(&net_tx_id), reinterpret_cast<uint8_t*>(&net_tx_id) + sizeof(uint64_t));
        crc_data.push_back(net_type);
        crc_data.insert(crc_data.end(), reinterpret_cast<uint8_t*>(&payload_len), reinterpret_cast<uint8_t*>(&payload_len) + sizeof(uint32_t));
        if (payload_len > 0) {
            crc_data.insert(crc_data.end(), payload.data(), payload.data() + payload_len);
        }

        uint32_t computed_crc = compute_crc32c(crc_data.data(), crc_data.size());
        if (computed_crc != stored_crc) {
            // CRC mismatch indicates corruption or partial write at the end of the log
            break;
        }

        RecordType type = static_cast<RecordType>(type_val);
        
        // Invoke application callback to redo/undo transaction operations
        if (apply_callback) {
            apply_callback(lsn, tx_id, type, payload.data(), payload_len);
        }

        // Update tracking LSNs
        if (lsn >= next_lsn_) {
            next_lsn_ = lsn + 1;
        }
        flushed_lsn_ = lsn;
    }
}

void WAL::flush_buffer_locked() {
    if (bytes_written_in_buffer_ == 0) {
        return;
    }

    size_t bytes_to_write = bytes_written_in_buffer_;
    bytes_written_in_buffer_ = 0;

    // Perform raw OS write
    const uint8_t* ptr = write_buffer_.data();
    size_t written = 0;
    while (written < bytes_to_write) {
        ssize_t n = ::write(fd_, ptr + written, bytes_to_write - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "Failed to write to WAL file");
        }
        written += n;
    }

    // Issue fsync for durability
    if (::fsync(fd_) < 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to fsync WAL file");
    }

    // Extract the max LSN contained in this flushed batch
    // Since records are sequentially packed, we scan or track the highest LSN written.
    // For safety and correctness, we update flushed_lsn_ to next_lsn_ - 1 or parse precisely.
    LSN current_max_lsn = next_lsn_ - 1;
    flushed_lsn_.store(current_max_lsn, std::memory_order_release);
    cv_.notify_all();
}

void WAL::background_writer_loop() {
    std::unique_lock<std::mutex> lock(mutex_,érant); // standard unique_lock
    while (true) {
        cv_.wait_for(lock, std::chrono::milliseconds(10), [this] {
            return stop_writer_ || bytes_written_in_buffer_ > 0;
        });

        if (stop_writer_ && bytes_written_in_buffer_ == 0) {
            break;
        }

        try {
            flush_buffer_locked();
        } catch (...) {
            // Log or handle background write failure gracefully if needed
        }
    }
}

void WAL::recover_last_lsn() {
    // Quick pass to find the highest LSN in the existing WAL file on startup
    off_t current_pos = ::lseek(fd_, 0, SEEK_CUR);
    ::lseek(fd_, 0, SEEK_SET);

    while (true) {
        uint64_t lsn = 0;
        uint64_t tx_id = 0;
        uint8_t type_val = 0;
        uint32_t stored_crc = 0;
        uint32_t payload_len = 0;

        ssize_t n = ::read(fd_, &lsn, sizeof(uint64_t));
        if (n <= 0) break;
        if (::read(fd_, &tx_id, sizeof(uint64_t)) != sizeof(uint64_t)) break;
        if (::read(fd_, &type_val, sizeof(uint8_t)) != sizeof(uint8_t)) break;
        if (::read(fd_, &stored_crc, sizeof(uint32_t)) != sizeof(uint32_t)) break;
        if (::read(fd_, &payload_len, sizeof(uint32_t)) != sizeof(uint32_t)) break;

        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0) {
            if (::read(fd_, payload.data(), payload_len) != static_cast<ssize_t>(payload_len)) {
                break;
            }
        }

        // Verify CRC
        std::vector<uint8_t> crc_data;
        crc_data.reserve(sizeof(uint64_t) + sizeof(