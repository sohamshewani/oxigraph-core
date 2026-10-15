#ifndef OXIGRAPH_BTREE_HPP
#define OXIGRAPH_BTREE_HPP

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <new>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <vector>
#include <array>
#include <memory>
#include <system_error>
#include <stdexcept>
#include <algorithm>
#include <optional>
#include <span>
#include <thread>
#include <chrono>

namespace oxigraph {

using page_id_t = uint64_t;
using lsn_t = uint64_t;
using tx_id_t = uint64_t;
using version_t = uint64_t;

static constexpr size_t PAGE_SIZE = 4096;
static constexpr page_id_t INVALID_PAGE_ID = 0;
static constexpr lsn_t INVALID_LSN = 0;

enum class ErrorCode : int {
    Success = 0,
    BufferPoolExhausted,
    PageNotFound,
    PagePinned,
    WalSyncFailed,
    CorruptedPage,
    KeyNotFound,
    DuplicateKey,
    NodeOverflow
};

class BTreeError : public std::system_error {
public:
    explicit BTreeError(ErrorCode ec) 
        : std::system_error(static_cast<int>(ec), std::generic_category(), get_error_message(ec)) {}
    
    explicit BTreeError(ErrorCode ec, const std::string& what_arg)
        : std::system_error(static_cast<int>(ec), std::generic_category(), what_arg) {}

private:
    static const char* get_error_message(ErrorCode ec) {
        switch (ec) {
            case ErrorCode::Success: return "Success";
            case ErrorCode::BufferPoolExhausted: return "Buffer pool exhausted";
            case ErrorCode::PageNotFound: return "Page not found";
            case ErrorCode::PagePinned: return "Page is pinned";
            case ErrorCode::WalSyncFailed: return "WAL synchronization failed";
            case ErrorCode::CorruptedPage: return "Corrupted page detected";
            case ErrorCode::KeyNotFound: return "Key not found";
            case ErrorCode::DuplicateKey: return "Duplicate key";
            case ErrorCode::NodeOverflow: return "B-Tree node overflow";
        }
        return "Unknown error";
    }
};

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

enum class WalRecordType : uint8_t {
    Insert = 1,
    Update = 2,
    Delete = 3,
    Split = 4,
    Merge = 5,
    Checkpoint = 6
};

#pragma pack(push, 1)
struct WalHeader {
    lsn_t lsn;
    tx_id_t tx_id;
    WalRecordType type;
    uint32_t crc32;
    uint32_t payload_length;
};
#pragma pack(pop)

class WAL {
public:
    explicit WAL(const std::string& log_path) : current_lsn_(1), flushed_lsn_(0) {}
    ~WAL() = default;

    lsn_t append(tx_id_t tx_id, WalRecordType type, const uint8_t* payload, size_t length) {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        lsn_t assigned_lsn = current_lsn_++;
        
        WalHeader header;
        header.lsn = assigned_lsn;
        header.tx_id = tx_id;
        header.type = type;
        header.payload_length = static_cast<uint32_t>(length);
        
        std::vector<uint8_t> buffer(sizeof(WalHeader) + length);
        std::memcpy(buffer.data(), &header, sizeof(WalHeader));
        if (length > 0 && payload != nullptr) {
            std::memcpy(buffer.data() + sizeof(WalHeader), payload, length);
        }
        
        header.crc32 = CRC32C::calculate(buffer.data() + sizeof(WalHeader) - 4 - 4, length + 4); 
        // Simplified CRC check calculation span
        std::memcpy(buffer.data() + offsetof(WalHeader, crc32), &header.crc32, sizeof(uint32_t));

        log_buffer_.insert(log_buffer_.end(), buffer.begin(), buffer.end());
        return assigned_lsn;
    }

    void flush_to(lsn_t target_lsn) {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        if (target_lsn > flushed_lsn_) {
            // Simulate synchronous disk flush
            flushed_lsn_ = target_lsn;
        }
    }

    lsn_t get_flushed_lsn() const noexcept {
        return flushed_lsn_.load(std::memory_order_acquire);
    }

private:
    mutable std::mutex wal_mutex_;
    std::atomic<lsn_t> current_lsn_;
    std::atomic<lsn_t> flushed_lsn_;
    std::vector<uint8_t> log_buffer_;
};

struct alignas(4096) Page {
    uint8_t data[PAGE_SIZE];

    Page() {
        std::memset(data, 0, PAGE_SIZE);
    }
};

struct FrameDescriptor {
    page_id_t page_id{INVALID_PAGE_ID};
    std::atomic<int32_t> pin_count{0};
    std::atomic<lsn_t> page_lsn{INVALID_LSN};
    bool is_dirty{false};
    std::atomic<bool> is_valid{false};
    std::shared_mutex rw_lock;
};

class BufferPoolManager {
public:
    explicit BufferPoolManager(size_t pool_size, std::shared_ptr<WAL> wal)
        : pool_size_(pool_size), frames_(pool_size), pages_(pool_size), wal_(std::move(wal)), shut_down_(false) {
        flusher_thread_ = std::thread(&BufferPoolManager::background_flusher, this);
    }

    ~BufferPoolManager() {
        {
            std::unique_lock<std::mutex> lock(flush_mutex_);
            shut_down_ = true;
        }
        flush_cv_.notify_all();
        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }
    }

    Page* fetch_page(page_id_t page_id) {
        size_t frame_index = find_or_evict_frame(page_id);
        FrameDescriptor& frame = frames_[frame_index];
        frame.pin_count.fetch_add(1, std::memory_order_acq_rel);
        return &pages_[frame_index];
    }

    void unpin_page(page_id_t page_id, bool is_dirty) {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].page_id == page_id) {
                if (is_dirty) {
                    frames_[i].is_dirty = true;
                }
                frames_[i].pin_count.fetch_sub(1, std::memory_order_acq_rel);
                return;
            }
        }
        throw BTreeError(ErrorCode::PageNotFound);
    }

    void flush_all() {
        for (size_t i = 0; i < pool_size_; ++i) {
            FrameDescriptor& frame = frames_[i];
            if (frame.is_valid.load(std::memory_order_acquire) && frame.is_dirty) {
                lsn_t page_lsn = frame.page_lsn.load(std::memory_order_acquire);
                wal_->flush_to(page_lsn);
                frame.is_dirty = false;
            }
        }
    }

private:
    size_t find_or_evict_frame(page_id_t page_id) {
        for (size_t i = 0; i < pool_size_; ++i) {
            if (frames_[i].page_id == page_id && frames_[i].is_valid.load(std::memory_order_acquire)) {
                return i;
            }
        }

        // Simple Clock/LRU eviction approximation
        for (size_t i = 0; i < pool_size_; ++i) {
            size_t idx = (evict_rover_ + i) % pool_size_;
            FrameDescriptor& frame = frames_[idx];
            if (!frame.is_valid.load(std::memory_order_acquire)) {
                evict_rover_ = (idx + 1) % pool_size_;
                frame.page_id = page_id;
                frame.is_valid.store(true, std::memory_order_release);
                frame.is_dirty = false;
                frame.page_lsn.store(INVALID_LSN, std::memory_order_release);
                return idx;
            }
            int32_t expected_pins = 0;
            if (frame.pin_count.compare_exchange_strong(expected_pins, 0, std::memory_order_acq_rel)) {
                if (frame.is_dirty) {
                    wal_->flush_to(frame.page_lsn.load(std::memory_order_acquire));
                    frame.is_dirty = false;
                }
                evict_rover_ = (idx + 1) % pool_size_;
                frame.page_id = page_id;
                frame.is_valid.store(true, std::memory_order_release);
                frame.page_lsn.store(INVALID_LSN, std::memory_order_release);
                return idx;
            }
        }
        throw BTreeError(ErrorCode::BufferPoolExhausted);
    }

    void background_flusher() {
        while (true) {
            std::unique_lock<std::mutex> lock(flush_mutex_);
            flush_cv_.wait_for(lock, std::chrono::milliseconds(50), [this] { return shut_down_.load(); });
            if (shut_down_.load()) {
                flush_all();
                break;
            }
            flush_all();
        }
    }

    size_t pool_size_;
    std::vector<FrameDescriptor> frames_;
    std::vector<Page> pages_;
    std::shared_ptr<WAL> wal_;
    size_t evict_rover_{0};
    std::thread flusher_thread_;
    std::mutex flush_mutex_;
    std::condition_variable_any flush_cv_;
    std::atomic<bool> shut_down_;
};

enum class NodeType : uint8_t {
    Internal = 1,
    Leaf = 2
};

#pragma pack(push, 1)
struct NodeHeader {
    NodeType type;
    uint16_t count;
    uint16_t capacity;
    page_id_t next_page_id;
    std::atomic<version_t> node_version;
    lsn_t page_lsn;
};

struct Slot {
    uint16_t key_offset;
    uint16_t key_length;
    uint16_t value_offset;
    uint16_t value_length;
    page_id_t child_page_id; // For internal nodes
};
#pragma pack(pop)

class BTree {
public:
    explicit BTree(std::shared_ptr<BufferPoolManager> bpm, std::shared_ptr<WAL> wal, page_id_t root_page_id)
        : bpm_(std::move(bpm)), wal_(std::move(wal)), root_page_id_(root_page_id) {}

    ~BTree() = default;

    std::optional<std::vector<uint8_t>> search(std::span<const uint8_t> key) {
        page_id_t current_id = root_page_id_;

        while (true) {
            Page* page = bpm_->fetch_page(current_id);
            NodeHeader* header = reinterpret_cast<NodeHeader*>(page->data);

            version_t v1 = header->node_version.load(std::memory_order_acquire);
            if (v1 & 1) {
                bpm_->unpin_page(current_id, false);
                std::this_thread::yield();
                continue;
            }

            Slot* slots = reinterpret_cast<Slot*>(page->data + sizeof(NodeHeader));
            
            if (header->type == NodeType::Internal) {
                page_id_t next_id = INVALID_PAGE_ID;
                bool found_child = false;
                
                for (uint16_t i = 0; i < header->count; ++i) {
                    const uint8_t* k_ptr = page->data + slots[i].key_offset;
                    std::span<const uint8_t> current_key(k_ptr, slots[i].key_length);
                    
                    if (compare_keys(key, current_key) < 0) {
                        next_id = slots[i].child_page_id;
                        found_child = true;
                        break;
                    }
                }
                
                if (!found_child && header->count > 0) {
                    next_id = slots[header->count - 1].child_page_id;
                }

                version_t v2 = header->node_version.load(std::memory_order_acquire);
                bpm_->unpin_page(current_id, false);

                if (v1 != v2 || next_id == INVALID_PAGE_ID) {
                    continue; // Retry OCC validation
                }
                current_id = next_id;
            } else {
                std::optional<std::vector<uint8_t>> result;
                for (uint16_t i = 0; i < header->count; ++i) {
                    const uint8_t* k_ptr = page->data + slots[i].key_offset;
                    std::span<const uint8_t> current_key(k_ptr, slots[i].key_length);
                    
                    if (compare_keys(key, current_key) == 0) {
                        const uint8_t* v_ptr = page->data + slots[i].value_offset;
                        result = std::vector<uint8_t>(v_ptr, v_ptr + slots[i].value_length);
                        break;
                    }
                }

                version_t v2 = header->node_version.load(std::memory_order_acquire);
                bpm_->unpin_page(current_id, false);

                if (v1 != v2) {
                    continue;
                }
                return result;
            }
        }
    }

    void insert(tx_id_t tx_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        std::vector<uint8_t> wal_payload;
        wal_payload.insert(wal_payload.end(), key.begin(), key.end());
        wal_payload.insert(wal_payload.end(), value.begin(), value.end());
        lsn_t lsn = wal_->append(tx_id, WalRecordType::Insert, wal_payload.data(), wal_payload.size());
        wal_->flush_to(lsn);

        Page* root_page = bpm_->fetch_page(root_page_id_);
        NodeHeader* root_header = reinterpret_cast<NodeHeader*>(root_page->data);

        size_t required_space = sizeof(Slot) + key.size() + value.size();
        size_t free_