find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

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

SET(CPP_FILES
        ../../../src/filesystem/File.cpp
        ../../../src/checkpoint/FileBackedCheckpointCoordinator.cpp
        ../../../src/checkpoint/BlockManager.cpp
        ../../../src/checkpoint/LineageGraph.cpp
        ../../../src/cql/expressions/Expression.cpp
        ../../../src/dispatcher/ITaskDispatcher.cpp
        ../../../src/dispatcher/JoinTaskDispatcher.cpp
        ../../../src/dispatcher/TaskDispatcher.cpp
        ../../../src/compression/CompressionCodeGenUtils.cpp
        ../../../src/compression/CompressionStatistics.cpp
        ../../../src/monitors/CompressionMonitor.cpp
        ../../../src/monitors/PerformanceMonitor.cpp
        ../../../src/monitors/Measurement.cpp
        ../../../src/monitors/LatencyMonitor.cpp
        ../../../src/processor/TaskProcessor.cpp
        ../../../src/result/ResultHandler.cpp
        ../../../src/tasks/NumaTaskQueueWrapper.cpp
        ../../../src/tasks/WindowBatch.cpp
        ../../../src/tasks/Task.cpp
        ../../../src/utils/AttributeType.cpp
        ../../../src/utils/Query.cpp
        ../../../src/utils/QueryApplication.cpp
        ../../../src/utils/Utils.cpp
        ../../../src/utils/SystemConf.cpp
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DHAVE_Oo")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

link_directories(${LIBPMEMOBJ++_LIBRARY_DIRS})

# Yahoo Benchmark
add_executable(yahoo_benchmark_flink
        main.cpp
        ${CPP_FILES}
        )
target_link_options(yahoo_benchmark_flink PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(yahoo_benchmark_flink ${Boost_LIBRARIES})
endif ()
target_include_directories(yahoo_benchmark_flink PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(yahoo_benchmark_flink
        ${LIBPMEMOBJ++_LIBRARIES}
        operatorJITLib
        boost_fiber
        boost_system
        tbb snappy
        pthread dl aio uuid stdc++fs)
target_compile_options(yahoo_benchmark_flink PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(yahoo_benchmark_flink PROPERTIES COMPILE_FLAGS "-DHAVE_NUMA -DPREFETCH")


# Kafka Benchmarks
add_executable(kafka_benchmarks
        mainKafka.cpp
        ${CPP_FILES}
        )
target_link_options(kafka_benchmarks PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(kafka_benchmarks ${Boost_LIBRARIES})
endif ()
target_include_directories(kafka_benchmarks PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(kafka_benchmarks
        ${LIBPMEMOBJ++_LIBRARIES}
        operatorJITLib
        boost_fiber
        boost_system
        tbb snappy
        pthread dl aio uuid stdc++fs)
target_compile_options(kafka_benchmarks PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(kafka_benchmarks PROPERTIES COMPILE_FLAGS "-DHAVE_NUMA -DPREFETCH")