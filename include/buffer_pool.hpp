#ifndef OXIGRAPH_BUFFER_POOL_HPP
#define OXIGRAPH_BUFFER_POOL_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <condition_variable>
#include <vector>
#include <array>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace oxigraph {

constexpr size_t PAGE_SIZE = 4096;
using page_id_t = uint32_t;
using frame_id_t = int32_t;
using lsn_t = uint64_t;

constexpr page_id_t INVALID_PAGE_ID = 0xFFFFFFFF;
constexpr frame_id_t INVALID_FRAME_ID = -1;
constexpr lsn_t INVALID_LSN = 0;

enum class BufferPoolError {
    Success = 0,
    NoFreeFrames,
    PageNotFound,
    PagePinned,
    DiskReadError,
    DiskWriteError,
    OutOfMemory
};

class BufferPoolException : public std::system_error {
public:
    explicit BufferPoolException(BufferPoolError err, const std::string& msg = "")
        : std::system_error(static_cast<int>(err), std::generic_category(), msg), err_(err) {}
    
    BufferPoolError error_code() const { return err_; }
private:
    BufferPoolError err_;
};

struct alignas(PAGE_SIZE) Page {
    std::array<uint8_t, PAGE_SIZE> data;

    void reset() {
        std::memset(data.data(), 0, PAGE_SIZE);
    }
};

class DiskManager {
public:
    explicit DiskManager(const std::string& db_file) {
        fd_ = ::open(db_file.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            throw BufferPoolException(BufferPoolError::DiskReadError, "Failed to open database file");
        }
    }

    ~DiskManager() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    void read_page(page_id_t page_id, char* page_data) {
        off_t offset = static_cast<off_t>(page_id) * PAGE_SIZE;
        ssize_t bytes_read = ::pread(fd_, page_data, PAGE_SIZE, offset);
        if (bytes_read < 0) {
            throw BufferPoolException(BufferPoolError::DiskReadError, "Failed to read page from disk");
        }
        if (bytes_read < static_cast<ssize_t>(PAGE_SIZE)) {
            std::memset(page_data + bytes_read, 0, PAGE_SIZE - bytes_read);
        }
    }

    void write_page(page_id_t page_id, const char* page_data) {
        off_t offset = static_cast<off_t>(page_id) * PAGE_SIZE;
        ssize_t bytes_written = ::pwrite(fd_, page_data, PAGE_SIZE, offset);
        if (bytes_written != static_cast<ssize_t>(PAGE_SIZE)) {
            throw BufferPoolException(BufferPoolError::DiskWriteError, "Failed to write page to disk");
        }
    }

    void sync() {
        if (::fsync(fd_) < 0) {
            throw BufferPoolException(BufferPoolError::DiskWriteError, "Failed to sync database file");
        }
    }

private:
    int fd_;
};

class LogManager {
public:
    explicit LogManager(const std::string& log_file) : flushed_lsn_(0), current_lsn_(0) {
        fd_ = ::open(log_file.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            throw BufferPoolException(BufferPoolError::DiskReadError, "Failed to open WAL file");
        }
    }

    ~LogManager() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    lsn_t append_record(const uint8_t* data, size_t length) {
        std::lock_guard<std::mutex> lock(latch_);
        current_lsn_++;
        // WAL record format simulation
        off_t offset = ::lseek(fd_, 0, SEEK_END);
        if (offset < 0) {
            throw BufferPoolException(BufferPoolError::DiskWriteError, "Failed to seek WAL");
        }
        uint64_t lsn = current_lsn_;
        ::write(fd_, &lsn, sizeof(lsn));
        ::write(fd_, &length, sizeof(length));
        ::write(fd_, data, length);
        return current_lsn_;
    }

    void flush_to(lsn_t lsn) {
        std::lock_guard<std::mutex> lock(latch_);
        if (lsn <= flushed_lsn_) {
            return;
        }
        if (::fsync(fd_) < 0) {
            throw BufferPoolException(BufferPoolError::DiskWriteError, "Failed to fsync WAL");
        }
        flushed_lsn_ = lsn;
    }

    lsn_t get_flushed_lsn() const {
        return flushed_lsn_.load();
    }

private:
    int fd_;
    std::mutex latch_;
    std::atomic<lsn_t> flushed_lsn_;
    lsn_t current_lsn_;
};

struct FrameDescriptor {
    page_id_t page_id{INVALID_PAGE_ID};
    std::atomic<int32_t> pin_count{0};
    bool is_dirty{false};
    lsn_t page_lsn{INVALID_LSN};
    std::atomic_flag spinlock = ATOMIC_FLAG_INIT;

    void lock() {
        while (spinlock.test_and_set(std::memory_order_acquire)) {
            #if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
            #endif
        }
    }

    void unlock() {
        spinlock.clear(std::memory_order_release);
    }
};

class Replacer {
public:
    explicit Replacer(size_t num_frames) : num_frames_(num_frames), clock_hand_(0) {
        ref_bits_.resize(num_frames, false);
    }

    bool victim(frame_id_t* frame_id, const std::vector<FrameDescriptor>& frames) {
        std::lock_guard<std::mutex> lock(latch_);
        size_t scans = 0;
        while (scans < 2 * num_frames_) {
            frame_id_t fid = static_cast<frame_id_t>(clock_hand_);
            clock_hand_ = (clock_hand_ + 1) % num_frames_;
            scans++;

            // Check if frame can be evicted
            const auto& desc = frames[fid];
            if (desc.page_id == INVALID_PAGE_ID) {
                continue;
            }
            if (desc.pin_count.load() > 0) {
                continue;
            }

            if (ref_bits_[fid]) {
                ref_bits_[fid] = false;
            } else {
                *frame_id = fid;
                return true;
            }
        }
        return false;
    }

    void pin(frame_id_t frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        if (static_cast<size_t>(frame_id) < num_frames_) {
            ref_bits_[frame_id] = true;
        }
    }

    void unpin(frame_id_t frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        if (static_cast<size_t>(frame_id) < num_frames_) {
            ref_bits_[frame_id] = true;
        }
    }

private:
    size_t num_frames_;
    size_t clock_hand_;
    std::vector<bool> ref_bits_;
    std::mutex latch_;
};

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager, LogManager* log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager),
          pages_(pool_size), frames_(pool_size), replacer_(pool_size), stop_flusher_(false) {
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.push_back(static_cast<frame_id_t>(i));
        }
        flusher_thread_ = std::thread(&BufferPoolManager::background_flusher, this);
    }

    ~BufferPoolManager() {
        shutdown();
    }

    Page* fetch_page(page_id_t page_id) {
        std::unique_lock<std::shared_mutex> lock(pool_latch_);

        // 1. Search in page table
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id_t frame_id = it->second;
            auto& desc = frames_[frame_id];
            desc.lock();
            desc.pin_count.fetch_add(1, std::memory_order_relaxed);
            replacer_.pin(frame_id);
            desc.unlock();
            return &pages_[frame_id];
        }

        // 2. Get free frame or evict
        frame_id_t frame_id = INVALID_FRAME_ID;
        if (!free_list_.empty()) {
            frame_id = free_list_.back();
            free_list_.pop_back();
        } else {
            if (!replacer_.victim(&frame_id, frames_)) {
                throw BufferPoolException(BufferPoolError::NoFreeFrames, "No free or evictable frames in buffer pool");
            }
            auto& old_desc = frames_[frame_id];
            old_desc.lock();
            if (old_desc.page_id != INVALID_PAGE_ID) {
                // Ensure durability: wait for WAL to flush up to page LSN
                if (old_desc.is_dirty) {
                    log_manager_->flush_to(old_desc.page_lsn);
                    disk_manager_->write_page(old_desc.page_id, reinterpret_cast<char*>(pages_[frame_id].data.data()));
                }
                page_table_.erase(old_desc.page_id);
            }
            old_desc.unlock();
        }

        // 3. Initialize frame with new page
        auto& desc = frames_[frame_id];
        desc.lock();
        desc.page_id = page_id;
        desc.pin_count.store(1, std::memory_order_relaxed);
        desc.is_dirty = false;
        desc.page_lsn = INVALID_LSN;

        page_table_[page_id] = frame_id;
        desc.unlock();

        disk_manager_->read_page(page_id, reinterpret_cast<char*>(pages_[frame_id].data.data()));
        replacer_.pin(frame_id);

        return &pages_[frame_id];
    }

    bool unpin_page(page_id_t page_id, bool is_dirty) {
        std::shared_lock<std::shared_mutex> lock(pool_latch_);
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }

        frame_id_t frame_id = it->second;
        auto& desc = frames_[frame_id];
        desc.lock();

        if (desc.pin_count.load() <= 0) {
            desc.unlock();
            return false;
        }

        if (is_dirty) {
            desc.is_dirty = true;
        }

        int32_t count = desc.pin_count.fetch_sub(1, std::memory_order_relaxed) - 1;
        if (count == 0) {
            replacer_.unpin(frame_id);
        }

        desc.unlock();
        return true;
    }

    bool flush_page(page_id_t page_id) {
        std::shared_lock<std::shared_mutex> lock(pool_latch_);
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }

        frame_id_t frame_id = it->second;
        auto& desc = frames_[frame_id];
        desc.lock();

        if (desc.is_dirty) {
            log_manager_->flush_to(desc.page_lsn);
            disk_manager_->write_page(page_id, reinterpret_cast<char*>(pages_[frame_id].data.data()));
            desc.is_dirty = false;
        }

        desc.unlock();
        return true;
    }

    Page* new_page(page_id_t* page_id) {
        // Simple allocator logic for testing/integration
        static std::atomic<page_id_t> next_page_id{1};
        *page_id = next_page_id.fetch_add(1, std::memory_order_relaxed);
        return fetch_page(*page_id);
    }

    void flush_all() {
        std::shared_lock<std::shared_mutex> lock(pool_latch_);
        for (size_t i = 0; i < pool_size_; ++i) {
            auto& desc = frames_[i];
            desc.lock();
            if (desc.page_id != INVALID_PAGE_ID && desc.is_dirty) {
                log_manager_->flush_to(desc.page_lsn);
                disk_manager_->write_page(desc.page_id, reinterpret_cast<char*>(pages_[i].data.data()));
                desc.is_dirty = false;
            }
            desc.unlock();
        }
        disk_manager_->sync();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(flusher_mutex_);
            stop_flusher_ = true;
        }
        flusher_cv_.notify_all();
        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }
        flush_all();
    }

private:
    void background_flusher() {
        while (true) {
            std::unique_lock<std::mutex> lock(flusher_mutex_);
            if (flusher_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] { return stop_flusher_.load(); })) {
                break;
            }
            // Background asynchronous flush loop
            std::shared_lock<std::shared_mutex> pool_lock(pool_latch_);
            for (size_t i = 0; i < pool_size_; ++i) {
                auto& desc = frames_[i];
                desc.lock();
                if (desc.page_id != INVALID_PAGE_ID && desc.is_dirty && desc.pin_count.load() == 0) {
                    lsn_t lsn = desc.page_lsn;
                    page_id_t pid = desc.page_id;
                    // Copy page data locally to release frame lock during I/O
                    Page page_copy = pages_[i];
                    desc.unlock();

                    log_manager_->flush_to(lsn);
                    disk_manager_->write_page(pid, reinterpret_cast<char*>(page_copy.data.data()));

                    desc.lock();
                    if (desc.page_id == pid && desc.page_lsn == lsn) {
                        desc.is_dirty = false;
                    }
                }
                desc.unlock();
            }
        }
    }

    size_t pool_size_;
    DiskManager* disk