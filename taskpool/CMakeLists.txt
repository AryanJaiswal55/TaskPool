cmake_minimum_required(VERSION 3.16)
project(taskpool LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# e.g. -DTASKPOOL_SANITIZE=thread   or   -DTASKPOOL_SANITIZE=address,undefined
set(TASKPOOL_SANITIZE "" CACHE STRING "Sanitizers to enable")

find_package(Threads REQUIRED)

add_library(taskpool INTERFACE)
target_include_directories(taskpool INTERFACE include)
target_link_libraries(taskpool INTERFACE Threads::Threads)
target_compile_options(taskpool INTERFACE -Wall -Wextra -Wpedantic)
if(TASKPOOL_SANITIZE)
  target_compile_options(taskpool INTERFACE -fsanitize=${TASKPOOL_SANITIZE} -fno-omit-frame-pointer)
  target_link_options(taskpool INTERFACE -fsanitize=${TASKPOOL_SANITIZE})
endif()

add_executable(taskpool_tests tests/test_main.cpp)
target_link_libraries(taskpool_tests PRIVATE taskpool)

add_executable(taskpool_bench bench/bench.cpp)
target_link_libraries(taskpool_bench PRIVATE taskpool)

enable_testing()
add_test(NAME unit COMMAND taskpool_tests)

if(TASKPOOL_SANITIZE MATCHES "thread")
  set_tests_properties(unit PROPERTIES ENVIRONMENT
    "TSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tests/tsan.supp")
endif()
