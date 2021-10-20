include_directories(ClusterMonitoring)
include_directories(LinearRoadBenchmark)
include_directories(SmartGrid)
include_directories(YahooBenchmark)
include_directories(ManufacturingEquipment)
include_directories(RemoteBenchmark)
include_directories(Nexmark)
include_directories(../../../src/RDMA)
include_directories(RemoteBenchmark/RDMA)
add_subdirectory(RemoteBenchmark/RDMA)


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

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DHAVE_Oo")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

# ManufacturingEquipment
add_executable(manufacturing_equipment
        ManufacturingEquipment/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(manufacturing_equipment PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(manufacturing_equipment ${Boost_LIBRARIES})
endif ()
target_link_libraries(manufacturing_equipment
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(manufacturing_equipment PRIVATE -Wall -Wextra -O3 -march=native)


# Cluster Monitoring
add_executable(cluster_monitoring
        ClusterMonitoring/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(cluster_monitoring PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(cluster_monitoring ${Boost_LIBRARIES})
endif ()
target_link_libraries(cluster_monitoring
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(cluster_monitoring PRIVATE -Wall -Wextra -O3 -march=native)


# Linear Road Benchmark
add_executable(linear_road_benchmark
        LinearRoadBenchmark/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(linear_road_benchmark PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(linear_road_benchmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(linear_road_benchmark
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(linear_road_benchmark PRIVATE -Wall -Wextra -O3 -march=native)


# Yahoo Benchmark
add_executable(yahoo_benchmark
        YahooBenchmark/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(yahoo_benchmark PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(yahoo_benchmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(yahoo_benchmark
        operatorJITLib
        boost_fiber
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(yahoo_benchmark PRIVATE -Wall -Wextra -O3 -march=native)


# Smart Grid
add_executable(smartgrid
        SmartGrid/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(smartgrid ${Boost_LIBRARIES})
endif ()
target_link_libraries(smartgrid
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(smartgrid PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(smartgrid PRIVATE -Wl,--unresolved-symbols=ignore-all)


#  Nexmark
add_executable(nexmark
        Nexmark/main.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(nexmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(nexmark
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(nexmark PRIVATE -Wall -Wextra -O3 -march=native -DHAVE_NUM)
target_link_options(nexmark PRIVATE -Wl,--unresolved-symbols=ignore-all)


# Remote source
add_executable(remoteSource
        RemoteBenchmark/remoteSource.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(remoteSource ${Boost_LIBRARIES})
endif ()
target_link_libraries(remoteSource
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(remoteSource PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(remoteSource PRIVATE -Wl,--unresolved-symbols=ignore-all)


# Remote sink
add_executable(remoteSink
        RemoteBenchmark/remoteSink.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(remoteSource ${Boost_LIBRARIES})
endif ()
target_link_libraries(remoteSink
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(remoteSink PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(remoteSink PRIVATE -Wl,--unresolved-symbols=ignore-all)

# Remote RDMA source
add_executable(remoteRDMASource
        RemoteBenchmark/remoteRDMASource.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(remoteRDMASource ${Boost_LIBRARIES})
endif ()
target_link_libraries(remoteRDMASource
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(remoteRDMASource PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(remoteRDMASource PRIVATE -Wl,--unresolved-symbols=ignore-all)


# Remote RDMA sink
add_executable(remoteRDMASink
        RemoteBenchmark/remoteRDMASink.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(remoteSource ${Boost_LIBRARIES})
endif ()
target_link_libraries(remoteRDMASink
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(remoteRDMASink PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(remoteRDMASink PRIVATE -Wl,--unresolved-symbols=ignore-all)