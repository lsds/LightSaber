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
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

if(NOT WIN32)
    find_package(PkgConfig QUIET) # PATHS /usr/local/lib64/pkgconfig /usr/local/lib/pkgconfig)
endif()

SET(ENV{PKG_CONFIG_PATH} "/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
message(STATUS "PKG_CONFIG_PATH: $ENV{PKG_CONFIG_PATH}")

if(PKG_CONFIG_FOUND)
    pkg_check_modules(LIBPMEMOBJ++ REQUIRED libpmemobj++)
else()
    find_package(LIBPMEMOBJ++ REQUIRED)
endif()

link_directories(${LIBPMEMOBJ++_LIBRARY_DIRS})

# Operator Microbenchmarks
add_executable(TestProjection
        TestProjection.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestProjection PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestGAG ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestProjection
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestProjection PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestSelection
        TestSelection.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestSelection PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestSelection ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestSelection
        ${LIBPMEMOBJ++_LIBRARIES}
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestSelection PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestAggregation
        TestAggregation.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestAggregation PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestAggregation ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestAggregation
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestAggregation PRIVATE -Wall -Wextra -O3 -march=native)

# GAG standalone Microbenchmarks
add_executable(TestGAG
        TestGAG.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestGAG PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestGAG ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestGAG
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestGAG PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestJoin
        TestJoin.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestJoin PRIVATE -Wl,--unresolved-symbols=ignore-all)
target_include_directories(TestJoin PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestJoin ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestJoin
        ${LIBPMEMOBJ++_LIBRARIES}
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestJoin PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestPersistentProjection
        TestPersistentProjection.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestPersistentProjection PRIVATE -Wl,--unresolved-symbols=ignore-all)
target_include_directories(TestPersistentProjection PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestJoin ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestPersistentProjection
        ${LIBPMEMOBJ++_LIBRARIES}
        z snappy
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestPersistentProjection PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)

add_executable(TestPersistentAggregation
        TestPersistentAggregation.cpp
        ${CPP_FILES} ${RDMA_CPP_FILES}
        )
target_link_options(TestPersistentAggregation PRIVATE -Wl,--unresolved-symbols=ignore-all)
target_include_directories(TestPersistentAggregation PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestJoin ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestPersistentAggregation
        ${LIBPMEMOBJ++_LIBRARIES}
        z snappy
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(TestPersistentAggregation PRIVATE -Wall -Wextra -O3 -march=native -UNDEBUG)