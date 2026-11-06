#pragma once
// Bounded multi-producer / multi-consumer lock-free queue (Dmitry Vyukov's design).
//
// Every cell carries a sequence number that encodes whose turn it is:
//   seq == pos        -> cell is free for the producer that owns ticket `pos`
//   seq == pos + 1    -> cell holds data for the consumer that owns ticket `pos`
//   seq == pos + cap  -> cell has been consumed, free again one lap later
// Producers and consumers claim tickets with a CAS on tail/head; the seq
// store (release) / load (acquire) pair publishes the element itself.
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace taskpool {

template <class T>
class MPMCQueue {
    static_assert(std::is_nothrow_move_constructible<T>::value &&
                  std::is_nothrow_move_assignable<T>::value,
                  "T must be nothrow movable: a throw after a ticket is claimed would "
                  "leave its cell permanently stuck");
    static constexpr std::size_t kCacheLine = 64;

public:
    // Capacity is rounded up to a power of two (minimum 2).
    explicit MPMCQueue(std::size_t capacity)
        : mask_(round_up_pow2(capacity < 2 ? 2 : capacity) - 1),
          cells_(std::make_unique<Cell[]>(mask_ + 1)) {
        for (std::size_t i = 0; i <= mask_; ++i) cells_[i].seq.store(i, std::memory_order_relaxed);
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }
    MPMCQueue(const MPMCQueue&) = delete;
    MPMCQueue& operator=(const MPMCQueue&) = delete;

    ~MPMCQueue() {  // destroy anything still queued
        T tmp;
        while (try_pop(tmp)) {}
    }

    std::size_t capacity() const noexcept { return mask_ + 1; }

    // Moves `v` into the queue and returns true, or returns false (v untouched) if full.
    bool try_push(T&& v) {
        std::size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& c = cells_[pos & mask_];
            const std::size_t seq = c.seq.load(std::memory_order_acquire);
            const std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    ::new (static_cast<void*>(c.storage)) T(std::move(v));
                    c.seq.store(pos + 1, std::memory_order_release);  // publish
                    return true;
                }
            } else if (diff < 0) {
                return false;  // consumer has not freed this cell yet: full
            } else {
                pos = tail_.load(std::memory_order_relaxed);  // another producer got ahead
            }
        }
    }

    // Moves the oldest element into `out` and returns true, or returns false if empty.
    bool try_pop(T& out) {
        std::size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& c = cells_[pos & mask_];
            const std::size_t seq = c.seq.load(std::memory_order_acquire);
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
            if (diff == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    T* p = reinterpret_cast<T*>(c.storage);
                    out = std::move(*p);
                    p->~T();
                    c.seq.store(pos + mask_ + 1, std::memory_order_release);  // free for next lap
                    return true;
                }
            } else if (diff < 0) {
                return false;  // producer has not filled this cell yet: empty
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

private:
    struct Cell {
        std::atomic<std::size_t> seq;
        alignas(T) unsigned char storage[sizeof(T)];
    };
    static std::size_t round_up_pow2(std::size_t n) {
        std::size_t p = 1;
        while (p < n) p <<= 1;
        return p;
    }

    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    // head and tail on separate cache lines: producers and consumers must not
    // invalidate each other's line on every operation (false sharing).
    alignas(kCacheLine) std::atomic<std::size_t> head_;
    alignas(kCacheLine) std::atomic<std::size_t> tail_;
};

} // namespace taskpool
