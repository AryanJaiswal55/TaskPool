#pragma once
// LockFreePool: same interface as ThreadPool, but tasks travel through the
// lock-free MPMCQueue. A mutex/condvar exists ONLY so idle workers can sleep
// instead of burning CPU; the hot path (submit + pop) takes no lock when
// workers are busy.
//
// Wake-up protocol (no lost wake-ups):
//   producer: push; ++available_ ; if (sleepers_ > 0) { lock; unlock; notify }
//   sleeper : lock; ++sleepers_ ; wait(pred: stop || available_ > 0)
// Both sides use seq_cst, so either the sleeper sees available_ > 0 or the
// producer sees sleepers_ > 0 (store-buffering / Dekker argument).
//
// Contract: do not call submit() concurrently with shutdown().
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "taskpool/mpmc_queue.hpp"
#include "taskpool/task.hpp"
#include "taskpool/thread_pool.hpp"  // make_task

namespace taskpool {

class LockFreePool {
public:
    explicit LockFreePool(std::size_t threads = std::thread::hardware_concurrency(),
                          std::size_t queue_capacity = 1 << 14)
        : queue_(queue_capacity) {
        if (threads == 0) threads = 1;
        for (std::size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { worker(); });
    }
    LockFreePool(const LockFreePool&) = delete;
    LockFreePool& operator=(const LockFreePool&) = delete;
    ~LockFreePool() { shutdown(); }

    // If the queue is full the caller yields until a slot frees up (back-pressure).
    template <class F, class... A>
    auto submit(F&& f, A&&... a) {
        if (stop_.load(std::memory_order_acquire)) throw std::runtime_error("LockFreePool: submit after shutdown");
        auto [task, fut] = make_task(std::forward<F>(f), std::forward<A>(a)...);
        unfinished_.fetch_add(1, std::memory_order_acq_rel);  // before push, so wait_idle never misses it
        while (!queue_.try_push(std::move(task))) std::this_thread::yield();
        available_.fetch_add(1);                               // seq_cst on purpose
        if (sleepers_.load() > 0) {
            { std::lock_guard<std::mutex> lk(m_); }            // serialise with a sleeper about to wait
            cv_.notify_one();
        }
        return std::move(fut);
    }

    void wait_idle() {
        std::unique_lock<std::mutex> lk(idle_m_);
        idle_cv_.wait(lk, [this] { return unfinished_.load(std::memory_order_acquire) == 0; });
    }

    void shutdown() {
        if (stop_.exchange(true)) return;
        { std::lock_guard<std::mutex> lk(m_); }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

    std::size_t thread_count() const noexcept { return workers_.size(); }

private:
    static constexpr int kSpins = 64;

    void run(Task& t) {
        available_.fetch_sub(1);
        t();
        t.reset();
        if (unfinished_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            { std::lock_guard<std::mutex> lk(idle_m_); }
            idle_cv_.notify_all();
        }
    }

    void worker() {
        Task t;
        for (;;) {
            if (queue_.try_pop(t)) { run(t); continue; }
            bool got = false;
            for (int i = 0; i < kSpins && !got; ++i) {  // brief spin before sleeping
                if (queue_.try_pop(t)) got = true; else std::this_thread::yield();
            }
            if (got) { run(t); continue; }

            std::unique_lock<std::mutex> lk(m_);
            if (stop_.load() && available_.load() <= 0) return;
            sleepers_.fetch_add(1);
            cv_.wait(lk, [this] { return stop_.load() || available_.load() > 0; });
            sleepers_.fetch_sub(1);
            if (stop_.load() && available_.load() <= 0) return;
        }
    }

    MPMCQueue<Task> queue_;
    std::atomic<long> available_{0};   // items pushed but not yet claimed (may dip below 0 transiently)
    std::atomic<long> sleepers_{0};
    std::atomic<std::size_t> unfinished_{0};
    std::atomic<bool> stop_{false};
    std::mutex m_, idle_m_;
    std::condition_variable cv_, idle_cv_;
    std::vector<std::thread> workers_;
};

} // namespace taskpool
