#pragma once
// Move-only type-erased `void()` callable. std::function cannot hold a
// std::packaged_task (it is move-only), so the pools use this instead.
#include <memory>
#include <type_traits>
#include <utility>

namespace taskpool {

class Task {
    struct Concept {
        virtual ~Concept() = default;
        virtual void call() = 0;
    };
    template <class F>
    struct Model final : Concept {
        explicit Model(F&& f) : fn(std::move(f)) {}
        void call() override { fn(); }
        F fn;
    };

public:
    Task() noexcept = default;
    template <class F, class D = std::decay_t<F>,
              class = std::enable_if_t<!std::is_same<D, Task>::value>>
    Task(F&& f) : impl_(new Model<D>(D(std::forward<F>(f)))) {}

    Task(Task&&) noexcept = default;
    Task& operator=(Task&&) noexcept = default;

    explicit operator bool() const noexcept { return static_cast<bool>(impl_); }
    void operator()() { impl_->call(); }
    void reset() noexcept { impl_.reset(); }

private:
    std::unique_ptr<Concept> impl_;
};

} // namespace taskpool
