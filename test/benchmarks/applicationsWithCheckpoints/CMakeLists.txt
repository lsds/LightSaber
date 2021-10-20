include_directories(ClusterMonitoring)
include_directories(LinearRoadBenchmark)
include_directories(SmartGrid)
include_directories(YahooBenchmark)
include_directories(ManufacturingEquipment)
include_directories(Nexmark)
include_directories(../applications/RemoteBenchmark)
include_directories(../applications/RemoteBenchmark/RDMA)
include_directories(../../../src/RDMA)

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
        ../../../src/monitors/ThroughputMonitor.cpp
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
        ../../../src/filesystem/File.cpp
        ../../../src/checkpoint/FileBackedCheckpointCoordinator.cpp
        ../../../src/checkpoint/BlockManager.cpp
        ../../../src/checkpoint/LineageGraph.cpp
        )
SET(RDMA_CPP_FILES
        ../../../src/RDMA/infinity/core/Context.cpp
        ../../../src/RDMA/infinity/memory/Atomic.cpp
        ../../../src/RDMA/infinity/memory/Buffer.cpp
        ../../../src/RDMA/infinity/memory/Region.cpp
        ../../../src/RDMA/infinity/memory/RegionToken.cpp
        ../../../src/RDMA/infinity/memory/RegisteredMemory.cpp
        ../../../src/RDMA/infinity/queues/QueuePair.cpp
        ../../../src/RDMA/infinity/queues/QueuePairFactory.cpp
        ../../../src/RDMA/infinity/requests/RequestToken.cpp
        ../../../src/RDMA/infinity/utils/Address.cpp
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma -lrt")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DNO_DIS -DRDMA_INPU -DRDMA_OUTPU -DTCP_INPU -DTCP_OUTPU") # -DTCP_INPUT -DTCP_OUTPUT -DHAVE_NUMA -DHAVE_SHARE") --gcc-toolchain=/usr/local/gcc/7.5.0

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

if(NOT WIN32)
    find_package(PkgConfig QUIET)
endif()

SET(ENV{PKG_CONFIG_PATH} "/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
message(STATUS "PKG_CONFIG_PATH: $ENV{PKG_CONFIG_PATH}")

if(PKG_CONFIG_FOUND)
    pkg_check_modules(LIBPMEMOBJ++ REQUIRED libpmemobj++)
else()
    find_package(LIBPMEMOBJ++ REQUIRED)
endif()

link_directories(${LIBPMEMOBJ++_LIBRARY_DIRS})

# Yahoo Benchmark
add_executable(yahoo_benchmark_checkpoints
        YahooBenchmark/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
target_link_options(yahoo_benchmark_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(yahoo_benchmark_checkpoints ${Boost_LIBRARIES})
endif ()
target_include_directories(yahoo_benchmark_checkpoints PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(yahoo_benchmark_checkpoints
        ${LIBPMEMOBJ++_LIBRARIES}
        operatorJITLib
        boost_fiber
        boost_system
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(yahoo_benchmark_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(yahoo_benchmark_checkpoints PROPERTIES COMPILE_FLAGS "-DHAVE_NUM -DPREFETCH")


# ManufacturingEquipment
add_executable(manufacturing_equipment_checkpoints
        ManufacturingEquipment/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
target_link_options(manufacturing_equipment_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(manufacturing_equipment_checkpoints ${Boost_LIBRARIES} ${LIBPMEMOBJ++_INCLUDE_DIRS})
endif ()
target_link_libraries(manufacturing_equipment_checkpoints
        z ${LIBPMEMOBJ++_LIBRARIES}
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(manufacturing_equipment_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)


# Cluster Monitoring
add_executable(cluster_monitoring_checkpoints
        ClusterMonitoring/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
target_link_options(cluster_monitoring_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(cluster_monitoring_checkpoints ${Boost_LIBRARIES} ${LIBPMEMOBJ++_INCLUDE_DIRS})
endif ()
target_link_libraries(cluster_monitoring_checkpoints
        z ${LIBPMEMOBJ++_LIBRARIES}
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(cluster_monitoring_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(cluster_monitoring_checkpoints PROPERTIES COMPILE_FLAGS "-DHAVE_NUM -DPREFETCH")


# Linear Road Benchmark
add_executable(linear_road_benchmark_checkpoints
        LinearRoadBenchmark/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
target_link_options(linear_road_benchmark_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(linear_road_benchmark_checkpoints ${Boost_LIBRARIES} ${LIBPMEMOBJ++_INCLUDE_DIRS})
endif ()
target_link_libraries(linear_road_benchmark_checkpoints
        z ${LIBPMEMOBJ++_LIBRARIES}
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(linear_road_benchmark_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(linear_road_benchmark_checkpoints PROPERTIES COMPILE_FLAGS "-DHAVE_NUM -DPREFETCH")


# Smart Grid
add_executable(smartgrid_checkpoints
        SmartGrid/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(smartgrid_checkpoints ${Boost_LIBRARIES} ${LIBPMEMOBJ++_INCLUDE_DIRS})
endif ()
target_link_libraries(smartgrid_checkpoints
        z ${LIBPMEMOBJ++_LIBRARIES}
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(smartgrid_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
target_link_options(smartgrid_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
set_target_properties(smartgrid_checkpoints PROPERTIES COMPILE_FLAGS "-DHAVE_NUM -DPREFETCH")


# Nexmark
add_executable(nexmark_checkpoints
        Nexmark/main.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
target_link_options(nexmark_checkpoints PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(nexmark_checkpoints ${Boost_LIBRARIES} ${LIBPMEMOBJ++_INCLUDE_DIRS})
endif ()
target_link_libraries(nexmark_checkpoints
        z ${LIBPMEMOBJ++_LIBRARIES}
        operatorJITLib
        boost_fiber
        boost_system
        boost_iostreams
        tbb snappy ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(nexmark_checkpoints PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)
set_target_properties(nexmark_checkpoints PROPERTIES COMPILE_FLAGS "-DHAVE_NUM -DPREFETCH")