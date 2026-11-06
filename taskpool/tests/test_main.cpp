// Dependency-free test runner. Build with -DTASKPOOL_SANITIZE=thread (and address,undefined).
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "taskpool/lockfree_pool.hpp"
#include "taskpool/mpmc_queue.hpp"
#include "taskpool/thread_pool.hpp"

using namespace taskpool;

static std::atomic<int> g_fail{0}, g_checks{0};
#define CHECK(cond)                                                                      \
    do {                                                                                 \
        ++g_checks;                                                                      \
        if (!(cond)) { ++g_fail; std::cerr << "  FAIL " << __FILE__ << ":" << __LINE__  \
                                           << "  " #cond "\n"; }                        \
    } while (0)

static std::vector<std::pair<std::string, std::function<void()>>>& registry() {
    static std::vector<std::pair<std::string, std::function<void()>>> r;
    return r;
}
struct Reg { Reg(const char* n, std::function<void()> f) { registry().push_back({n, std::move(f)}); } };
#define TEST(name) static void name(); static Reg reg_##name(#name, name); static void name()

// ------------------------------------------------------------------ queue
TEST(queue_fifo_full_empty_and_wraparound) {
    MPMCQueue<int> q(5);
    CHECK(q.capacity() == 8);  // rounded up to a power of two
    int out = -1;
    CHECK(!q.try_pop(out));
    for (int i = 0; i < 8; ++i) { int v = i; CHECK(q.try_push(std::move(v))); }
    int extra = 99;
    CHECK(!q.try_push(std::move(extra)));  // full
    CHECK(extra == 99);                    // a failed push must not consume the value
    for (int i = 0; i < 8; ++i) { CHECK(q.try_pop(out)); CHECK(out == i); }
    CHECK(!q.try_pop(out));
    // many laps around the ring
    int next_in = 0, next_out = 0;
    for (int lap = 0; lap < 10000; ++lap) {
        for (int k = 0; k < 3; ++k) { int v = next_in++; CHECK(q.try_push(std::move(v))); }
        for (int k = 0; k < 3; ++k) { CHECK(q.try_pop(out)); CHECK(out == next_out++); }
    }
}

static std::atomic<int> g_live{0};
struct Counted {
    Counted() { ++g_live; }
    explicit Counted(int v) : v(v) { ++g_live; }
    Counted(Counted&& o) noexcept : v(o.v) { ++g_live; }
    Counted& operator=(Counted&& o) noexcept { v = o.v; return *this; }
    ~Counted() { --g_live; }
    int v = 0;
};

TEST(queue_destroys_leftover_elements) {
    {
        MPMCQueue<Counted> q(16);
        for (int i = 0; i < 10; ++i) { Counted c(i); CHECK(q.try_push(std::move(c))); }
        Counted out;
        CHECK(q.try_pop(out) && out.v == 0);
    }
    CHECK(g_live.load() == 0);
}

TEST(queue_mpmc_every_item_exactly_once_and_per_producer_order) {
    constexpr int kProducers = 4, kConsumers = 4, kPerProducer = 100000;
    MPMCQueue<std::uint64_t> q(1024);  // small ring: forces constant full/empty contention
    std::atomic<int> consumed{0};
    std::vector<std::vector<std::uint64_t>> got(kConsumers);

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p)
        threads.emplace_back([&, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                std::uint64_t v = (static_cast<std::uint64_t>(p) << 32) | static_cast<std::uint32_t>(i);
                while (!q.try_push(std::move(v))) std::this_thread::yield();
            }
        });
    for (int c = 0; c < kConsumers; ++c)
        threads.emplace_back([&, c] {
            std::uint64_t v;
            while (consumed.load() < kProducers * kPerProducer) {
                if (q.try_pop(v)) { got[c].push_back(v); consumed.fetch_add(1); }
                else std::this_thread::yield();
            }
        });
    for (auto& t : threads) t.join();

    // (1) each (producer, seq) pair was delivered exactly once
    std::vector<char> seen(static_cast<std::size_t>(kProducers) * kPerProducer, 0);
    std::size_t total = 0;
    for (auto& g : got)
        for (std::uint64_t v : g) {
            const std::size_t idx = static_cast<std::size_t>(v >> 32) * kPerProducer + (v & 0xffffffffu);
            CHECK(seen[idx] == 0);
            seen[idx] = 1;
            ++total;
        }
    CHECK(total == static_cast<std::size_t>(kProducers) * kPerProducer);
    // (2) any single consumer sees each producer's items in increasing order (FIFO)
    for (auto& g : got) {
        std::vector<std::int64_t> last(kProducers, -1);
        for (std::uint64_t v : g) {
            const int p = static_cast<int>(v >> 32);
            const std::int64_t s = static_cast<std::int64_t>(v & 0xffffffffu);
            CHECK(s > last[p]);
            last[p] = s;
        }
    }
}

// ------------------------------------------------------------------ pools (run for both)
template <class Pool>
void pool_returns_results() {
    Pool pool(4);
    std::vector<std::future<long>> fs;
    for (int i = 0; i < 5000; ++i) fs.push_back(pool.submit([](long x) { return x * x; }, static_cast<long>(i)));
    long sum = 0;
    for (auto& f : fs) sum += f.get();
    long expect = 0;
    for (long i = 0; i < 5000; ++i) expect += i * i;
    CHECK(sum == expect);
}

template <class Pool>
void pool_propagates_exceptions() {
    Pool pool(2);
    auto ok = pool.submit([] { return 7; });
    auto bad = pool.submit([]() -> int { throw std::runtime_error("boom"); });
    CHECK(ok.get() == 7);
    bool caught = false;
    try { bad.get(); } catch (const std::runtime_error& e) { caught = std::string(e.what()) == "boom"; }
    CHECK(caught);
    CHECK(pool.submit([] { return 1; }).get() == 1);  // pool survives a throwing task
}

template <class Pool>
void pool_wait_idle_waits_for_everything() {
    Pool pool(4);
    std::atomic<int> done{0};
    for (int i = 0; i < 2000; ++i)
        pool.submit([&] { std::this_thread::sleep_for(std::chrono::microseconds(50)); done.fetch_add(1); });
    pool.wait_idle();
    CHECK(done.load() == 2000);
}

template <class Pool>
void pool_shutdown_drains_queue_and_rejects_new_work() {
    std::atomic<int> done{0};
    {
        Pool pool(2);
        for (int i = 0; i < 500; ++i)
            pool.submit([&] { std::this_thread::sleep_for(std::chrono::microseconds(100)); done.fetch_add(1); });
        pool.shutdown();
        CHECK(done.load() == 500);  // queued tasks were drained, not dropped
        bool threw = false;
        try { pool.submit([] {}); } catch (const std::runtime_error&) { threw = true; }
        CHECK(threw);
        pool.shutdown();  // idempotent
    }
}

template <class Pool>
void pool_nested_and_concurrent_submission() {
    Pool pool(4);
    std::atomic<int> count{0};
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p)
        producers.emplace_back([&] {
            for (int i = 0; i < 1000; ++i)
                pool.submit([&] {
                    count.fetch_add(1);
                    pool.submit([&] { count.fetch_add(1); });  // a task that submits a task
                });
        });
    for (auto& t : producers) t.join();
    pool.wait_idle();
    CHECK(count.load() == 8000);
}

template <class Pool>
void pool_single_thread_preserves_submission_order() {
    Pool pool(1);
    std::vector<int> order;
    std::mutex m;
    for (int i = 0; i < 1000; ++i) pool.submit([&, i] { std::lock_guard<std::mutex> lk(m); order.push_back(i); });
    pool.wait_idle();
    std::vector<int> expect(1000);
    std::iota(expect.begin(), expect.end(), 0);
    CHECK(order == expect);
}

template <class Pool>
void pool_idle_workers_wake_up_after_sleeping() {
    Pool pool(3);
    for (int round = 0; round < 20; ++round) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));  // let workers go to sleep
        std::atomic<int> n{0};
        for (int i = 0; i < 10; ++i) pool.submit([&] { n.fetch_add(1); });
        pool.wait_idle();
        CHECK(n.load() == 10);
    }
}

#define POOL_TESTS(Pool, tag)                                                                 \
    TEST(tag##_returns_results)           { pool_returns_results<Pool>(); }                  \
    TEST(tag##_propagates_exceptions)     { pool_propagates_exceptions<Pool>(); }            \
    TEST(tag##_wait_idle)                 { pool_wait_idle_waits_for_everything<Pool>(); }   \
    TEST(tag##_shutdown_drains)           { pool_shutdown_drains_queue_and_rejects_new_work<Pool>(); } \
    TEST(tag##_nested_and_concurrent)     { pool_nested_and_concurrent_submission<Pool>(); } \
    TEST(tag##_single_thread_order)       { pool_single_thread_preserves_submission_order<Pool>(); } \
    TEST(tag##_wakes_from_sleep)          { pool_idle_workers_wake_up_after_sleeping<Pool>(); }

POOL_TESTS(ThreadPool, mutex_pool)
POOL_TESTS(LockFreePool, lockfree_pool)

TEST(lockfree_pool_backpressure_with_tiny_queue) {
    LockFreePool pool(2, 4);  // 4-slot queue: submit() must block-and-retry, never drop
    std::atomic<int> n{0};
    for (int i = 0; i < 5000; ++i) pool.submit([&] { n.fetch_add(1); });
    pool.wait_idle();
    CHECK(n.load() == 5000);
}

// Usage: taskpool_tests [substring]   - run only tests whose name contains it.
int main(int argc, char** argv) {
    const std::string filter = argc > 1 ? argv[1] : "";
    for (auto& [name, fn] : registry()) {
        if (!filter.empty() && name.find(filter) == std::string::npos) continue;
        const int before = g_fail.load();
        fn();
        std::cout << (g_fail.load() == before ? "[ ok ] " : "[FAIL] ") << name << std::endl;
    }
    std::cout << "\n" << g_checks.load() << " checks, " << g_fail.load() << " failures\n";
    return g_fail.load() == 0 ? 0 : 1;
}
