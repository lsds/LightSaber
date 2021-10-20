find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma -lrt")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra")

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

SET(CPP_FILES
        ../../src/filesystem/File.cpp
        ../../src/checkpoint/FileBackedCheckpointCoordinator.cpp
        ../../src/checkpoint/BlockManager.cpp
        ../../src/checkpoint/LineageGraph.cpp
        ../../src/cql/expressions/Expression.cpp
        ../../src/dispatcher/ITaskDispatcher.cpp
        ../../src/dispatcher/JoinTaskDispatcher.cpp
        ../../src/dispatcher/TaskDispatcher.cpp
        ../../src/compression/CompressionCodeGenUtils.cpp
        ../../src/compression/CompressionStatistics.cpp
        ../../src/monitors/CompressionMonitor.cpp
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
SET(RDMA_CPP_FILES
        ../../src/RDMA/infinity/core/Context.cpp
        ../../src/RDMA/infinity/memory/Atomic.cpp
        ../../src/RDMA/infinity/memory/Buffer.cpp
        ../../src/RDMA/infinity/memory/Region.cpp
        ../../src/RDMA/infinity/memory/RegionToken.cpp
        ../../src/RDMA/infinity/memory/RegisteredMemory.cpp
        ../../src/RDMA/infinity/queues/QueuePair.cpp
        ../../src/RDMA/infinity/queues/QueuePairFactory.cpp
        ../../src/RDMA/infinity/requests/RequestToken.cpp
        ../../src/RDMA/infinity/utils/Address.cpp
        )

FIND_LIBRARY(tbb NAMES libtbb.so)

if(NOT WIN32)
    find_package(PkgConfig QUIET)
endif()

if(PKG_CONFIG_FOUND)
    pkg_check_modules(LIBPMEMOBJ++ REQUIRED libpmemobj++)
else()
    find_package(LIBPMEMOBJ++ REQUIRED)
endif()

link_directories(${LIBPMEMOBJ++_LIBRARY_DIRS})

add_executable(operators_unit_tests operators.cpp ${CPP_FILES} ${RDMA_CPP_FILES})
target_link_options(operators_unit_tests PRIVATE -Wl,--unresolved-symbols=ignore-all)
target_include_directories(operators_unit_tests PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(operators_unit_tests ${GTEST_LIBRARIES} ${LIBPMEMOBJ++_LIBRARIES}
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
#target_link_libraries(Unit_Tests_run ${benchmark_LIBRARIES} pthread tbb dl)
target_compile_options(operators_unit_tests PRIVATE -Wall -Wextra -O0 -march=native -UNDEBUG)

add_executable(internals_unit_tests internals.cpp ${CPP_FILES} ${RDMA_CPP_FILES})
target_include_directories(internals_unit_tests PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(internals_unit_tests ${GTEST_LIBRARIES} ${LIBPMEMOBJ++_LIBRARIES} pthread tbb dl boost_fiber aio uuid stdc++fs ibverbs)
target_compile_options(internals_unit_tests PRIVATE -Wall -Wextra -O0 -march=native -UNDEBUG)

add_executable(ds_unit_tests datastructures.cpp ${CPP_FILES} ${RDMA_CPP_FILES})
target_include_directories(ds_unit_tests PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
target_link_libraries(ds_unit_tests ${GTEST_LIBRARIES} ${LIBPMEMOBJ++_LIBRARIES} pthread tbb dl boost_fiber aio uuid stdc++fs ibverbs)
target_compile_options(ds_unit_tests PRIVATE -Wall -Wextra -O0 -march=native -UNDEBUG)

#add_executable(checkpoint_unit_tests checkpoint.cpp ${CPP_FILES} ../../src/filesystem/File.cpp ../../src/checkpoint/FileBackedCheckpointCoordinator.cpp  ${RDMA_CPP_FILES})
#target_include_directories(checkpoint_unit_tests PUBLIC ${LIBPMEMOBJ++_INCLUDE_DIRS})
#target_link_libraries(checkpoint_unit_tests ${GTEST_LIBRARIES} ${LIBPMEMOBJ++_LIBRARIES} pthread tbb dl boost_fiber aio uuid stdc++fs ibverbs)
#target_compile_options(checkpoint_unit_tests PRIVATE ${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -O0 -march=native)