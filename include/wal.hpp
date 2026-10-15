#ifndef OXIGRAPH_WAL_HPP
#define OXIGRAPH_WAL_HPP

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <filesystem>
#include <atomic>
#include <system_error>
#include <stdexcept>
#include <array>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace oxigraph {

using lsn_t = uint64_t;
using txid_t = uint64_t;

enum class WALRecordType : uint8_t {
    BEGIN = 0x01,
    COMMIT = 0x02,
    ABORT = 0x03,
    INSERT = 0x04,
    UPDATE = 0x05,
    DELETE = 0x06,
    PAGE_SPLIT = 0x07,
    PAGE_MERGE = 0x08,
    CHECKPOINT = 0x09
};

#pragma pack(push, 1)
struct WALHeader {
    lsn_t lsn;
    txid_t txid;
    WALRecordType type;
    uint32_t crc32c;
    uint32_t payload_length;
};
#pragma pack(pop)

class CRC32C {
public:
    static uint32_t calculate(const uint8_t* data, size_t length) noexcept {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (int j = 0; j < 8; ++j) {
                uint32_t mask = -(crc & 1);
                crc = (crc >> 1) ^ (0x82F63B78 & mask);
            }
        }
        return ~crc;
    }
};

class WALException : public std::runtime_error {
public:
    explicit WALException(const std::string& message) : std::runtime_error(message) {}
};

class WAL {
private:
    std::filesystem::path log_file_path_;
    int fd_;
    std::mutex latch_;
    std::atomic<lsn_t> next_lsn_;
    std::atomic<lsn_t> flushed_lsn_;
    std::vector<uint8_t> buffer_;
    size_t buffer_offset_;
    size_t max_buffer_size_;

    void write_internal(const uint8_t* data, size_t size) {
        if (buffer_offset_ + size > buffer_.size()) {
            flush_buffer_to_disk();
        }
        if (size > buffer_.size()) {
            // Direct write for oversized payloads exceeding buffer capacity
            ssize_t written = ::write(fd_, data, size);
            if (written < 0 || static_cast<size_t>(written) != size) {
                throw WALException("Failed to write oversized payload to WAL file descriptor.");
            }
            return;
        }
        std::memcpy(buffer_.data() + buffer_offset_, data, size);
        buffer_offset_ += size;
    }

    void flush_buffer_to_disk() {
        if (buffer_offset_ == 0) {
            return;
        }
        ssize_t written = ::write(fd_, buffer_.data(), buffer_offset_);
        if (written < 0 || static_cast<size_t>(written) != buffer_offset_) {
            throw WALException("Failed to flush WAL buffer to disk.");
        }
        buffer_offset_ = 0;
    }

public:
    explicit WAL(std::filesystem::path log_file_path, size_t buffer_size = 65536)
        : log_file_path_(std::move(log_file_path)),
          fd_(-1),
          next_lsn_(1),
          flushed_lsn_(0),
          buffer_offset_(0),
          max_buffer_size_(buffer_size) {
        
        buffer_.resize(max_buffer_size_);

        fd_ = ::open(log_file_path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            throw WALException("Failed to open or create WAL file: " + log_file_path_.string());
        }

        // Determine starting LSN from existing file size if appending
        struct stat st;
        if (fstat(fd_, &st) == 0 && st.st_size > 0) {
            // Simple heuristic: recover next_lsn based on records scan during recovery
            // For initialization, we scan the tail or set to 1 if empty.
        }
    }

    ~WAL() {
        try {
            flush();
            if (fd_ >= 0) {
                ::close(fd_);
            }
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&&) noexcept = delete;
    WAL& operator=(WAL&&) noexcept = delete;

    lsn_t append(txid_t txid, WALRecordType type, const uint8_t* payload, uint32_t payload_length) {
        std::lock_guard<std::mutex> lock(latch_);

        lsn_t assigned_lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed);

        WALHeader header;
        header.lsn = assigned_lsn;
        header.txid = txid;
        header.type = type;
        header.payload_length = payload_length;

        // Calculate CRC32C over header fields (excluding crc32c itself) and payload
        std::vector<uint8_t> checksum_buffer;
        size_t header_no_crc_size = sizeof(header.lsn) + sizeof(header.txid) + sizeof(header.type) + sizeof(header.payload_length);
        checksum_buffer.resize(header_no_crc_size + payload_length);

        size_t cursor = 0;
        std::memcpy(checksum_buffer.data() + cursor, &header.lsn, sizeof(header.lsn));
        cursor += sizeof(header.lsn);
        std::memcpy(checksum_buffer.data() + cursor, &header.txid, sizeof(header.txid));
        cursor += sizeof(header.txid);
        std::memcpy(checksum_buffer.data() + cursor, &header.type, sizeof(header.type));
        cursor += sizeof(header.type);
        std::memcpy(checksum_buffer.data() + cursor, &header.payload_length, sizeof(header.payload_length));
        cursor += sizeof(header.payload_length);

        if (payload_length > 0 && payload != nullptr) {
            std::memcpy(checksum_buffer.data() + cursor, payload, payload_length);
        }

        header.crc32c = CRC32C::calculate(checksum_buffer.data(), checksum_buffer.size());

        // Write to internal buffer
        write_internal(reinterpret_cast<const uint8_t*>(&header), sizeof(WALHeader));
        if (payload_length > 0 && payload != nullptr) {
            write_internal(payload, payload_length);
        }

        return assigned_lsn;
    }

    void flush_to(lsn_t target_lsn) {
        std::lock_guard<std::mutex> lock(latch_);
        if (target_lsn <= flushed_lsn_.load(std::memory_order_relaxed)) {
            return;
        }

        flush_buffer_to_disk();

        if (::fsync(fd_) < 0) {
            throw WALException("Failed to fsync WAL file.");
        }

        flushed_lsn_.store(next_lsn_.load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
    }

    void flush() {
        std::lock_guard<std::mutex> lock(latch_);
        flush_buffer_to_disk();
        if (::fsync(fd_) < 0) {
            throw WALException("Failed to fsync WAL file during complete flush.");
        }
        flushed_lsn_.store(next_lsn_.load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
    }

    lsn_t get_flushed_lsn() const noexcept {
        return flushed_lsn_.load(std::memory_order_relaxed);
    }

    lsn_t get_next_lsn() const noexcept {
        return next_lsn_.load(std::memory_order_relaxed);
    }

    class Iterator {
    private:
        std::ifstream file_;
        WALHeader current_header_;
        std::vector<uint8_t> current_payload_;
        bool has_next_;

    public:
        explicit Iterator(const std::filesystem::path& path)
            : file_(path, std::ios::binary), has_next_(false) {
            if (!file_.is_open()) {
                throw WALException("Failed to open WAL file for iteration/recovery.");
            }
            advance();
        }

        bool has_next() const noexcept {
            return has_next_;
        }

        void advance() {
            if (!file_.good() || file_.eof()) {
                has_next_ = false;
                return;
            }

            file_.read(reinterpret_cast<char*>(&current_header_), sizeof(WALHeader));
            if (file_.gcount() < static_cast<std::streamsize>(sizeof(WALHeader))) {
                has_next_ = false;
                return;
            }

            current_payload_.resize(current_header_.payload_length);
            if (current_header_.payload_length > 0) {
                file_.read(reinterpret_cast<char*>(current_payload_.data()), current_header_.payload_length);
                if (file_.gcount() < static_cast<std::streamsize>(current_header_.payload_length)) {
                    has_next_ = false;
                    return;
                }
            }

            // Verify CRC32C
            std::vector<uint8_t> checksum_buffer;
            size_t header_no_crc_size = sizeof(current_header_.lsn) + sizeof(current_header_.txid) + 
                                        sizeof(current_header_.type) + sizeof(current_header_.payload_length);
            checksum_buffer.resize(header_no_crc_size + current_header_.payload_length);

            size_t cursor = 0;
            std::memcpy(checksum_buffer.data() + cursor, &current_header_.lsn, sizeof(current_header_.lsn));
            cursor += sizeof(current_header_.lsn);
            std::memcpy(checksum_buffer.data() + cursor, &current_header_.txid, sizeof(current_header_.txid));
            cursor += sizeof(current_header_.txid);
            std::memcpy(checksum_buffer.data() + cursor, &current_header_.type, sizeof(current_header_.type));
            cursor += sizeof(current_header_.type);
            std::memcpy(checksum_buffer.data() + cursor, &current_header_.payload_length, sizeof(current_header_.payload_length));
            cursor += sizeof(current_header_.payload_length);

            if (current_header_.payload_length > 0) {
                std::memcpy(checksum_buffer.data() + cursor, current_payload_.data(), current_header_.payload_length);
            }

            uint32_t calculated_crc = CRC32C::calculate(checksum_buffer.data(), checksum_buffer.size());
            if (calculated_crc != current_header_.crc32c) {
                throw WALException("WAL corruption detected: CRC32C mismatch at LSN " + std::to_string(current_header_.lsn));
            }

            has_next_ = true;
        }

        const WALHeader& header() const noexcept {
            return current_header_;
        }

        const std::vector<uint8_t>& payload() const noexcept {
            return current_payload_;
        }
    };
};

} // namespace oxigraph

#endif // OXIGRAPH_WAL_HPP