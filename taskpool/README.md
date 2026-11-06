# taskpool

A C++17 concurrency toolkit built from primitives: a **bounded lock-free MPMC queue** and
**two thread pools** on top of it (a classic mutex/condvar baseline and a lock-free one), so
the two designs can be compared directly.

| Header | What it is |
|---|---|
| `mpmc_queue.hpp` | `MPMCQueue<T>`: bounded multi-producer/multi-consumer lock-free ring buffer (Vyukov's sequence-number design), `std::atomic` with explicit memory orders, head/tail on separate cache lines |
| `task.hpp` | `Task`: move-only type-erased `void()` callable (needed because `std::function` cannot hold a `std::packaged_task`) |
| `thread_pool.hpp` | `ThreadPool`: mutex + condition_variable queue; `submit()` returns a `std::future`, exceptions propagate through it, `shutdown()` drains the queue, `wait_idle()` |
| `lockfree_pool.hpp` | `LockFreePool`: same API, tasks flow through `MPMCQueue`; spin-then-sleep workers with a lost-wake-up-free protocol; back-pressure when the queue is full |

```cpp
#include "taskpool/lockfree_pool.hpp"

taskpool::LockFreePool pool(8);
auto f = pool.submit([](int a, int b) { return a + b; }, 20, 22);
int answer = f.get();     // 42; a throwing task rethrows here
pool.wait_idle();
```

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/taskpool_bench                      # throughput; run on a multi-core machine

# sanitizers (CI runs all three configurations)
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DTASKPOOL_SANITIZE=thread
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DTASKPOOL_SANITIZE=address,undefined
./build-tsan/taskpool_tests mpmc            # optional filter: run tests whose name contains "mpmc"
```

## How the queue works

Each cell has a sequence number that says whose turn it is: `seq == pos` means "free for the
producer holding ticket `pos`", `seq == pos + 1` means "holds data for the consumer holding
ticket `pos`", and `seq == pos + capacity` means "consumed, free again next lap". Producers and
consumers claim tickets with a CAS on `tail` / `head`; the release-store / acquire-load on `seq`
publishes the element itself. `try_push` / `try_pop` never block and never allocate.

The element type must be nothrow-movable: if a constructor threw *after* a ticket was claimed,
that cell could never advance and the queue would stall forever (enforced with `static_assert`).

## How the lock-free pool sleeps without losing wake-ups

Workers spin briefly, then sleep on a condition variable. Producer: `push; ++available;
if (sleepers > 0) { lock; unlock; notify }`. Sleeper: `lock; ++sleepers; wait(stop || available > 0)`.
Both counters are `seq_cst`, so either the sleeper sees `available > 0` or the producer sees
`sleepers > 0` (store-buffering argument). The empty lock/unlock makes the producer wait until a
sleeper that has already decided to sleep is actually inside `wait`, so the notify cannot be missed.
The mutex is only on the idle path; with busy workers `submit` and `pop` take no lock.

## How correctness is checked

- **Queue stress test:** 4 producers x 4 consumers x 100,000 items through a 1,024-slot ring
  (constant full/empty contention). Verifies every item is delivered **exactly once** and that
  each consumer sees every producer's items **in order**.
- Pool tests (run identically against both pools): results via futures, exception propagation,
  `wait_idle`, shutdown drains queued work and rejects new work, tasks that submit tasks while
  4 threads submit concurrently, strict ordering with one worker, waking workers that went to
  sleep, and back-pressure through a 4-slot queue.
- Runs clean under **ThreadSanitizer**, **AddressSanitizer** and **UBSan**.
- One TSan suppression is used (`tests/tsan.supp`): libstdc++ keeps `std::exception_ptr`'s
  reference count inside its uninstrumented runtime, so TSan reports a false race when a worker
  thread is the last owner of a `packaged_task` whose exception the caller has read. I
  reproduced the same report with plain `std::packaged_task` + `std::thread` and no taskpool code.

## Benchmarks

`taskpool_bench` compares both pools against a sequential loop for tiny, small, and CPU-bound
tasks at several thread counts. I developed this in a single-core sandbox, so I have **no
meaningful multi-core numbers to quote**: run it on your machine and report your own results.
Single-core runs only show per-task overhead (the lock-free pool had roughly 1.2-2.7x the
throughput of the mutex pool on near-empty tasks, and the two are indistinguishable for
CPU-bound work).

## Limitations

- `Task` heap-allocates every task (a small-buffer optimisation would remove that).
- One shared queue: no work stealing, so very uneven task sizes can leave workers idle.
- `LockFreePool::submit` must not run concurrently with `shutdown()`.
- Tasks run via `submit` capture exceptions in the future; there is no fire-and-forget `post`.
- Bounded queue: a full queue makes `submit` spin-yield (back-pressure) rather than grow.

## Ideas for extending it

Work-stealing deques per worker, a small-buffer `Task`, priorities, `parallel_for` / `parallel_reduce`
helpers, `std::atomic::wait` (C++20) instead of the condition variable, NUMA-aware placement.
