#ifndef OXIGRAPH_CORE_TYPES_HPP
#define OXIGRAPH_CORE_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>
#include <system_error>
#include <cstring>
#include <string_view>

namespace oxigraph::core {

// --- Basic Aliases and Constants ---
using lsn_t = uint64_t;
using txid_t = uint64_t;
using page_id_t = uint64_t;
using slot_offset_t = uint16_t;

constexpr size_t PAGE_SIZE = 4096;
constexpr page_id_t INVALID_PAGE_ID = 0xFFFFFFFFFFFFFFFFULL;
constexpr lsn_t INVALID_LSN = 0;
constexpr txid_t INVALID_TXID = 0;

// --- Error Handling Hierarchy ---
enum class ErrorCode : int {
    Success = 0,
    BufferPoolExhausted,
    PageNotFound,
    PagePinned,
    WalWriteError,
    WalSyncError,
    CorruptionDetected,
    ConcurrentModification,
    OutOfMemory,
    InvalidArguments
};

class SystemErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "oxigraph_core";
    }

    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            case ErrorCode::Success: return "Success";
            case ErrorCode::BufferPoolExhausted: return "Buffer pool exhausted";
            case ErrorCode::PageNotFound: return "Page not found in buffer pool";
            case ErrorCode::PagePinned: return "Page is pinned and cannot be evicted";
            case ErrorCode::WalWriteError: return "Failed to write to Write-Ahead Log";
            case ErrorCode::WalSyncError: return "Failed to sync Write-Ahead Log to disk";
            case ErrorCode::CorruptionDetected: return "Data corruption detected via CRC32C or invariant check";
            case ErrorCode::ConcurrentModification: return "OCC validation failed due to concurrent modification";
            case ErrorCode::OutOfMemory: return "Out of system or arena memory";
            case ErrorCode::InvalidArguments: return "Invalid arguments passed to subsystem";
            default: return "Unknown oxigraph core error";
        }
    }
};

inline const std::error_category& system_error_category() {
    static SystemErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) {
    return {static_cast<int>(e), system_error_category()};
}

} // namespace oxigraph::core

namespace std {
template <>
struct is_error_code_enum<oxigraph::core::ErrorCode> : true_type {};
}

namespace oxigraph::core {

// --- WAL Record Types and Structures ---
enum class WalRecordType : uint8_t {
    Begin = 0x01,
    Commit = 0x02,
    Abort = 0x03,
    Insert = 0x04,
    Update = 0x05,
    Delete = 0x06,
    Split = 0x07,
    Merge = 0x08,
    Checkpoint = 0x09
};

#pragma pack(push, 1)
struct WalHeader {
    lsn_t lsn;
    txid_t txid;
    uint8_t type;
    uint32_t crc32c;
    uint32_t payload_length;
};
#pragma pack(pop)

static_assert(sizeof(WalHeader) == 25, "WalHeader size must be strictly 25 bytes packed.");

// --- Memory Management & Page Layout ---
alignas(4096) struct Page {
    uint8_t data[PAGE_SIZE];

    void* operator new(size_t size) {
        if (size != PAGE_SIZE) {
            throw std::bad_alloc();
        }
        void* ptr = nullptr;
#if defined(_MSC_VER)
        ptr = _aligned_malloc(PAGE_SIZE, 4096);
#else
        int res = posix_memalign(&ptr, 4096, PAGE_SIZE);
        if (res != 0) {
            ptr = nullptr;
        }
#endif
        if (!ptr) {
            throw std::bad_alloc();
        }
        return ptr;
    }

    void operator delete(void* ptr) noexcept {
        if (ptr) {
#if defined(_MSC_VER)
            _aligned_free(ptr);
#else
            free(ptr);
#endif
        }
    }
};

static_assert(sizeof(Page) == PAGE_SIZE, "Page size must be exactly 4096 bytes.");
static_assert(alignof(Page) == 4096, "Page alignment must be strictly 4096 bytes.");

// --- B-Tree Node Structures ---
#pragma pack(push, 1)
struct BTreeNodeHeader {
    std::atomic<uint64_t> node_version;
    lsn_t page_lsn;
    page_id_t page_id;
    uint16_t flags;
    uint16_t num_keys;
    uint32_t free_space_offset;
};
#pragma pack(pop)

static_assert(sizeof(BTreeNodeHeader) == 32, "BTreeNodeHeader must be 32 bytes.");

struct SlotReference {
    slot_offset_t offset;
    slot_offset_t length;
};

// --- Concurrency & Frame Descriptors ---
struct FrameDescriptor {
    page_id_t page_id{INVALID_PAGE_ID};
    std::atomic<int32_t> pin_count{0};
    std::atomic<bool> is_dirty{false};
    lsn_t page_lsn{INVALID_LSN};
    uint32_t lru_timestamp{0};
    uint32_t sharded_lock_index{0};
};

// --- OCC Version Stamped Guard ---
class OCCNodeGuard {
public:
    explicit OCCNodeGuard(const volatile std::atomic<uint64_t>& version_counter)
        : version_counter_(&version_counter), 
          initial_version_(version_counter_.load(std::memory_order_acquire)) {
        if ((initial_version_ & 1ULL) != 0) {
            // Node is currently being modified, force initial validation failure
            initial_version_--; 
        }
    }

    ~OCCNodeGuard() = default;

    bool validate() const noexcept {
        uint64_t current_version = version_counter_->load(std::memory_order_acquire);
        return current_version == initial_version_;
    }

    uint64_t current_version() const noexcept {
        return initial_version_;
    }

    OCCNodeGuard(const OCCNodeGuard&) = delete;
    OCCNodeGuard& operator=(const OCCNodeGuard&) = delete;
    OCCNodeGuard(OCCNodeGuard&&) noexcept = default;
    OCCNodeGuard& operator=(OCCNodeGuard&&) noexcept = default;

private:
    const volatile std::atomic<uint64_t>* version_counter_;
    uint64_t initial_version_;
};

} // namespace oxigraph::core

#endif // OXIGRAPH_CORE_TYPES_HPP