// Throughput benchmark: tiny tasks (queue overhead dominates) vs CPU-bound tasks.
// Run in Release on your own machine; speed-ups depend on core count.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include "taskpool/lockfree_pool.hpp"
#include "taskpool/thread_pool.hpp"

using Clock = std::chrono::steady_clock;
static double seconds(Clock::time_point a, Clock::time_point b) { return std::chrono::duration<double>(b - a).count(); }

static volatile unsigned long g_sink;
[[gnu::noinline]] static unsigned long burn(int iters, unsigned long seed) {  // deterministic CPU work
    unsigned long x = 88172645463325252ull ^ (seed * 0x9E3779B97F4A7C15ull);  // seed: calls cannot be hoisted
    for (int i = 0; i < iters; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    return x;
}

template <class Pool>
static double run(Pool& pool, int tasks, int work) {
    std::atomic<unsigned long> acc{0};
    const auto t0 = Clock::now();
    for (int i = 0; i < tasks; ++i) pool.submit([&acc, work, i] { acc.fetch_add(burn(work, i), std::memory_order_relaxed); });
    pool.wait_idle();
    const auto t1 = Clock::now();
    g_sink = acc.load();
    return seconds(t0, t1);
}

int main() {
    const unsigned hw = std::thread::hardware_concurrency();
    std::printf("hardware threads: %u\n\n", hw);
    struct Case { const char* name; int tasks, work; } cases[] = {
        {"tiny tasks (~0 work)", 200000, 1},
        {"small tasks (~2k iters)", 100000, 2000},
        {"CPU-bound (~200k iters)", 2000, 200000},
    };
    for (auto& c : cases) {
        const auto s0 = Clock::now();
        unsigned long acc = 0;
        for (int i = 0; i < c.tasks; ++i) acc += burn(c.work, i);
        g_sink = acc;
        const double seq = seconds(s0, Clock::now());
        std::printf("%s  | %d tasks | sequential %.4fs (%.0f tasks/s)\n", c.name, c.tasks, seq, c.tasks / seq);
        std::vector<unsigned> counts{1, 2, 4};
        if (hw > 4) counts.push_back(hw);
        for (unsigned n : counts) {
            taskpool::ThreadPool a(n);
            const double ta = run(a, c.tasks, c.work);
            taskpool::LockFreePool b(n);
            const double tb = run(b, c.tasks, c.work);
            std::printf("   %2u threads   mutex pool %9.0f tasks/s (%.2fx)   lock-free pool %9.0f tasks/s (%.2fx)\n",
                        n, c.tasks / ta, seq / ta, c.tasks / tb, seq / tb);
        }
        std::printf("\n");
    }
}
