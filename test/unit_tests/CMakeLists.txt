find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

#find_package(benchmark REQUIRED)
#include_directories(${benchmark_INCLUDE_DIRS})

# Configure CCache if available
find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    message("Using CCache...")
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER "/usr/lib/ccache/clang++")
endif ()

add_executable(unit_tests_run
        main.cpp
        ../../src/dispatcher/TaskDispatcher.cpp
        ../../src/monitors/PerformanceMonitor.cpp
        ../../src/monitors/Measurement.cpp
        ../../src/monitors/LatencyMonitor.cpp
        ../../src/processor/TaskProcessor.cpp
        ../../src/result/ResultHandler.cpp
        ../../src/tasks/WindowBatch.cpp
        ../../src/tasks/Task.cpp
        ../../src/utils/AttributeType.cpp
        ../../src/utils/Query.cpp
        ../../src/utils/QueryApplication.cpp
        ../../src/utils/SystemConf.cpp
        ../../src/utils/Utils.cpp
        )

FIND_LIBRARY(tbb NAMES libtbb.so)

target_link_libraries(unit_tests_run ${GTEST_LIBRARIES} pthread tbb dl boost_fiber)
#target_link_libraries(Unit_Tests_run ${benchmark_LIBRARIES} pthread tbb dl)

target_compile_options(unit_tests_run PRIVATE -Wall -Wextra -O3 -march=native)

