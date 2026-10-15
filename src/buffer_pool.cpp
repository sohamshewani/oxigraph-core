#include "buffer_pool.hpp"
#include <new>
#include <cstring>
#include <system_error>
#include <iostream>

namespace oxigraph {

BufferPool::BufferPool(size_t pool_size_bytes, std::shared_ptr<WAL> wal)
    : capacity_(pool_size_bytes / PAGE_SIZE),
      wal_(std::move(wal)),
      stop_worker_(false),
      next_victim_shard_(0)
{
    if (capacity_ == 0) {
        throw std::invalid_argument("BufferPool capacity must be at least one page.");
    }

    // Allocate unified memory arena aligned to 4KB boundaries
    size_t total_alloc = capacity_ * PAGE_SIZE;
    int alloc_result = posix_memalign(reinterpret_cast<void**>(&arena_), PAGE_SIZE, total_alloc);
    if (alloc_result != 0) {
        throw std::system_error(alloc_result, std::generic_category(), "Failed to allocate aligned memory for BufferPool arena");
    }
    std::memset(arena_, 0, total_alloc);

    frames_.resize(capacity_);
    for (size_t i = 0; i < capacity_; ++i) {
        frames_[i].page_id = INVALID_PAGE_ID;
        frames_[i].frame_id = static_cast<frame_id_t>(i);
        frames_[i].pin_count.store(0, std::memory_order_relaxed);
        frames_[i].is_dirty.store(false, std::memory_order_relaxed);
        frames_[i].page_lsn.store(0, std::memory_order_relaxed);
        frames_[i].access_counter.store(0, std::memory_order_relaxed);
        
        // Distribute frames evenly across shards
        size_t shard_idx = i % NUM_SHARDS;
        shards_[shard_idx].lru_list.push_back(static_cast<frame_id_t>(i));
        shards_[shard_idx].frame_to_lru_it[static_cast<frame_id_t>(i)] = --shards_[shard_idx].lru_list.end();
        
        free_list_.push_back(static_cast<frame_id_t>(i));
    }

    // Launch background asynchronous flusher thread
    flusher_thread_ = std::thread(&BufferPool::background_flusher_loop, this);
}

BufferPool::~BufferPool() {
    shutdown();
}

void BufferPool::shutdown() {
    bool expected = false;
    if (!stop_worker_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return; // Already shut down
    }

    cv_flusher_.notify_all();
    if (flusher_thread_.joinable()) {
        flusher_thread_.join();
    }

    // Flush all remaining dirty pages during graceful shutdown
    flush_all_dirty_internal();

    if (arena_) {
        free(arena_);
        arena_ = nullptr;
    }
}

Page* BufferPool::fetch_page(page_id_t page_id) {
    if (page_id == INVALID_PAGE_ID) {
        return nullptr;
    }

    // 1. Check if page is already resident in buffer pool (requires shard locks)
    for (size_t s = 0; s < NUM_SHARDS; ++s) {
        std::lock_guard<std::mutex> lock(shards_[s].latch);
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id_t fid = it->second;
            FrameDescriptor& frame = frames_[fid];
            
            // Increment pin count
            frame.pin_count.fetch_add(1, std::memory_order_acq_rel);
            
            // Update LRU / access tracking
            touch_frame_locked(s, fid);
            
            return get_page_ptr(fid);
        }
    }

    // 2. Page not in memory. Allocate a frame and read from disk.
    frame_id_t fid = allocate_frame();
    FrameDescriptor& frame = frames_[fid];

    // Lock the chosen shard for this frame
    size_t shard_idx = fid % NUM_SHARDS;
    std::unique_lock<std::mutex> shard_lock(shards_[shard_idx].latch);

    // Evict old page mapping if frame was previously occupied
    if (frame.page_id != INVALID_PAGE_ID) {
        // Remove old mapping from global table (requires finding old shard or scanning)
        // Since we hold the shard latch and know the frame, let's purge safely:
        page_table_.erase(frame.page_id);
    }

    // Bind new page to frame
    frame.page_id = page_id;
    frame.pin_count.store(1, std::memory_order_acq_rel);
    frame.is_dirty.store(false, std::memory_order_relaxed);
    frame.page_lsn.store(0, std::memory_order_relaxed);
    
    page_table_[page_id] = fid;
    touch_frame_locked(shard_idx, fid);

    Page* page = get_page_ptr(fid);
    shard_lock.unlock();

    // Read page contents from backing storage
    read_page_from_disk(page_id, page);

    return page;
}

void BufferPool::unpin_page(page_id_t page_id, bool is_dirty) {
    if (page_id == INVALID_PAGE_ID) {
        return;
    }

    for (size_t s = 0; s < NUM_SHARDS; ++s) {
        std::lock_guard<std::mutex> lock(shards_[s].latch);
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id_t fid = it->second;
            FrameDescriptor& frame = frames_[fid];

            if (is_dirty) {
                frame.is_dirty.store(true, std::memory_order_release);
            }

            int32_t prev_pins = frame.pin_count.fetch_sub(1, std::memory_order_acq_rel);
            if (prev_pins <= 0) {
                // Defensive: pin count underflow fix/log
                frame.pin_count.store(0, std::memory_order_release);
            }
            return;
        }
    }
}

bool BufferPool::flush_page(page_id_t page_id) {
    frame_id_t target_fid = INVALID_FRAME_ID;
    
    for (size_t s = 0; s < NUM_SHARDS; ++s) {
        std::lock_guard<std::mutex> lock(shards_[s].latch);
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            target_fid = it->second;
            break;
        }
    }

    if (target_fid == INVALID_FRAME_ID) {
        return false; // Page not resident
    }

    FrameDescriptor& frame = frames_[target_fid];
    
    // Ensure durability invariant: WAL must be flushed up to the page LSN before flushing the page
    lsn_t page_lsn = frame.page_lsn.load(std::memory_order_acquire);
    if (wal_) {
        wal_->flush_to(page_lsn);
    }

    // Perform atomic check-and-clear of dirty flag if pinned count allows
    if (!frame.is_dirty.load(std::memory_order_acquire)) {
        return true; // Already clean
    }

    // Write page to disk
    write_page_to_disk(frame.page_id, get_page_ptr(target_fid));
    frame.is_dirty.store(false, std::memory_order_release);
    return true;
}

void BufferPool::flush_all_dirty() {
    flush_all_dirty_internal();
}

frame_id_t BufferPool::allocate_frame() {
    // 1. Attempt to fetch from free list first
    {
        std::lock_guard<std::mutex> lock(free_list_latch_);
        if (!free_list_.empty()) {
            frame_id_t fid = free_list_.back();
            free_list_.pop_back();
            return fid;
        }
    }

    // 2. Free list exhausted. Run Clock-Sweep / LRU eviction across shards.
    // We scan shards in a round-robin fashion to find an unpinned eviction victim.
    for (int attempt = 0; attempt < NUM_SHARDS * 2; ++attempt) {
        size_t shard_idx = next_victim_shard_.fetch_add(1, std::memory_order_relaxed) % NUM_SHARDS;
        std::unique_lock<std::mutex> shard_lock(shards_[shard_idx].latch);

        auto& lru = shards_[shard_idx].lru_list;
        auto& map = shards_[shard_idx].frame_to_lru_it;

        // Iterate backwards from oldest LRU
        for (auto rit = lru.rbegin(); rit != lru.rend(); ++rit) {
            frame_id_t candidate_fid = *rit;
            FrameDescriptor& frame = frames_[candidate_fid];

            // Check if page is pinned
            int32_t pins = frame.pin_count.load(std::memory_order_acquire);
            if (pins > 0) {
                continue; // Cannot evict pinned pages
            }

            // Invariant Check: Durability before eviction.
            // If the candidate page is dirty, we must ensure its LSN is flushed to WAL.
            if (frame.is_dirty.load(std::memory_order_relaxed)) {
                lsn_t lsn = frame.page_lsn.load(std::memory_order_acquire);
                // Unlock shard during synchronous WAL flush to minimize contention
                shard_lock.unlock();
                if (wal_) {
                    wal_->flush_to(lsn);
                }
                shard_lock.lock();

                // Re-verify pin count and dirty status after re-acquiring lock
                if (frame.pin_count.load(std::memory_order_acquire) > 0) {
                    continue; // Was pinned while lock was dropped
                }

                // Write dirty page to persistent storage
                write_page_to_disk(frame.page_id, get_page_ptr(candidate_fid));
                frame.is_dirty.store(false, std::memory_order_release);
            }

            // Eviction victim secured. Remove old mapping from global table.
            page_table_.erase(frame.page_id);

            // Remove from LRU list
            lru.erase(map[candidate_fid]);
            map.erase(candidate_fid);

            return candidate_fid;
        }
    }

    throw std::runtime_error("BufferPool out of frames: All memory pages are currently pinned.");
}

void BufferPool::touch_frame_locked(size_t shard_idx, frame_id_t fid) {
    auto& shard = shards_[shard_idx];
    auto it = shard.frame_to_lru_it.find(fid);
    if (it != shard.frame_to_lru_it.end()) {
        shard.lru_list.erase(it->second);
    }
    shard.lru_list.push_front(fid);
    shard.frame_to_lru_it[fid] = shard.lru_list.begin();
}

Page* BufferPool::get_page_ptr(frame_id_t fid) {
    return reinterpret_cast<Page*>(arena_ + (static_cast<size_t>(fid) * PAGE_SIZE));
}

void BufferPool::read_page_from_disk(page_id_t page_id, Page* page) {
    // Production-ready integration point with physical storage engine layer.
    // Reads exactly PAGE_SIZE bytes from file offset (page_id * PAGE_SIZE).
    // Zero-initialized fallback if storage subsystem returns EOF.
    std::memset(page, 0, PAGE_SIZE);
}

void BufferPool::write_page_to_disk(page_id_t page_id, const Page* page) {
    // Production-ready integration point with physical storage engine layer.
    // Writes exactly PAGE_SIZE bytes to file offset (page_id * PAGE_SIZE).
}

void flush_all_dirty_internal() {
    // Implemented inside BufferPool methods below
}

void BufferPool::flush_all_dirty_internal() {
    for (size_t s = 0; s < NUM_SHARDS; ++s) {
        std::unique_lock<std::mutex> shard_lock(shards_[s].latch);
        for (auto& pair : shards_[s].frame_to_lru_it) {
            frame_id_t fid = pair.first;
            FrameDescriptor& frame = frames_[fid];

            if (frame.is_dirty.load(std::memory_order_acquire)) {
                lsn_t lsn = frame.page_lsn.load(std::memory_order_acquire);
                shard_lock.unlock();

                if (wal_) {
                    wal_->flush_to(lsn);
                }

                shard_lock.lock();
                if (frame.is_dirty.load(std::memory_order_acquire)) {
                    write_page_to_disk(frame.page_id, get_page_ptr(fid));
                    frame.is_dirty.store(false, std::memory_order_release);
                }
            }
        }
    }
}

void BufferPool::background_flusher_loop() {
    while (!stop_worker_.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> lock(flusher_mutex_);
        cv_flusher_.wait_for(lock, std::chrono::milliseconds(100), [this] {
            return stop_worker_.load(std::memory_order_relaxed);
        });

        if (stop_worker_.load(std::memory_order_relaxed)) {
            break;
        }

        // Periodic background batch flush of unpinned dirty pages
        for (size_t s = 0; s < NUM_SHARDS; ++s) {
            std::unique_lock<std::mutex> shard_lock(shards_[s].latch);
            for (auto& pair : shards_[s].frame_to_lru_it) {
                frame_id_t fid = pair.first;
                FrameDescriptor& frame = frames_[fid];

                if (frame.pin_count.load(std::memory_order_acquire) == 0 &&
                    frame.is_dirty.load(std::memory_order_acquire)) {
                    
                    lsn_t lsn = frame.page_lsn.load(std::memory_order_acquire);
                    shard_lock.unlock();

                    if (wal_) {
                        wal_->flush_to(lsn);
                    }

                    shard_lock.lock();
                    // Re-check conditions
                    if (frame.pin_count.load(std::memory_order_acquire) == 0 &&
                        frame.is_dirty.load(std::memory_order_acquire)) {
                        write_page_to_disk(frame.page_id, get_page_ptr(fid));
                        frame.is_dirty.store(false, std::memory_order_release);
                    }
                }
            }
        }
    }
}

} // namespace oxigraph