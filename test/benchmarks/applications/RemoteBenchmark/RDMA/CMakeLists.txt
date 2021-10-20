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


SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DHAVE_Oo")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

SET(RDMA_CPP_FILES
        ../../../../../src/RDMA/infinity/core/Context.cpp
        ../../../../../src/RDMA/infinity/memory/Atomic.cpp
        ../../../../../src/RDMA/infinity/memory/Buffer.cpp
        ../../../../../src/RDMA/infinity/memory/Region.cpp
        ../../../../../src/RDMA/infinity/memory/RegionToken.cpp
        ../../../../../src/RDMA/infinity/memory/RegisteredMemory.cpp
        ../../../../../src/RDMA/infinity/queues/QueuePair.cpp
        ../../../../../src/RDMA/infinity/queues/QueuePairFactory.cpp
        ../../../../../src/RDMA/infinity/requests/RequestToken.cpp
        ../../../../../src/RDMA/infinity/utils/Address.cpp
        )

# read-write-send
add_executable(read-write-send
        examples/read-write-send.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(read-write-send ${Boost_LIBRARIES})
endif ()
target_link_libraries(read-write-send
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(read-write-send PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(read-write-send PRIVATE -Wl,--unresolved-symbols=ignore-all)


# send-performance
add_executable(send-performance
        examples/send-performance.cpp
        ${CPP_FILES}
        ${RDMA_CPP_FILES}
        )
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(send-performance ${Boost_LIBRARIES})
endif ()
target_link_libraries(send-performance
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl uuid stdc++fs)
target_compile_options(send-performance PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(send-performance PRIVATE -Wl,--unresolved-symbols=ignore-all)