#pragma once
// ThreadPool: classic mutex + condition_variable work queue (the baseline).
//
//  * submit() returns a std::future; exceptions thrown by the task travel through it
//  * shutdown() (also run by the destructor) DRAINS queued tasks, then joins
//  * wait_idle() blocks until every submitted task has finished
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include "taskpool/task.hpp"

namespace taskpool {

// Wrap f(args...) in a packaged_task and hand back its future. Shared by both pools.
template <class F, class... A>
auto make_task(F&& f, A&&... a) {
    using R = std::invoke_result_t<std::decay_t<F>, std::decay_t<A>...>;
    std::packaged_task<R()> pt(
        [fn = std::forward<F>(f), args = std::make_tuple(std::forward<A>(a)...)]() mutable -> R {
            return std::apply(std::move(fn), std::move(args));
        });
    std::future<R> fut = pt.get_future();
    return std::make_pair(Task(std::move(pt)), std::move(fut));
}

class ThreadPool {
public:
    explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency()) {
        if (threads == 0) threads = 1;
        for (std::size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { worker(); });
    }
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ~ThreadPool() { shutdown(); }

    template <class F, class... A>
    auto submit(F&& f, A&&... a) {
        auto [task, fut] = make_task(std::forward<F>(f), std::forward<A>(a)...);
        {
            std::lock_guard<std::mutex> lk(m_);
            if (stop_) throw std::runtime_error("ThreadPool: submit after shutdown");
            queue_.push_back(std::move(task));
        }
        cv_task_.notify_one();
        return std::move(fut);
    }

    void wait_idle() {
        std::unique_lock<std::mutex> lk(m_);
        cv_idle_.wait(lk, [this] { return queue_.empty() && active_ == 0; });
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(m_);
            if (stop_) return;
            stop_ = true;
        }
        cv_task_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

    std::size_t thread_count() const noexcept { return workers_.size(); }

private:
    void worker() {
        for (;;) {
            Task t;
            {
                std::unique_lock<std::mutex> lk(m_);
                cv_task_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (queue_.empty()) return;  // stop_ is set and nothing is left to drain
                t = std::move(queue_.front());
                queue_.pop_front();
                ++active_;
            }
            t();
            t.reset();  // release captured state before reporting idle
            {
                std::lock_guard<std::mutex> lk(m_);
                --active_;
                if (queue_.empty() && active_ == 0) cv_idle_.notify_all();
            }
        }
    }

    std::mutex m_;
    std::condition_variable cv_task_, cv_idle_;
    std::deque<Task> queue_;
    std::size_t active_ = 0;
    bool stop_ = false;
    std::vector<std::thread> workers_;
};

} // namespace taskpool
