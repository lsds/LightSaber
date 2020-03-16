cmake_minimum_required(VERSION 3.6)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(LightSaber)

include_directories(src)
include_directories(test)

add_subdirectory(src)
add_subdirectory(test)

set(CMAKE_CXX_STANDARD 17)

SET(HEADERS
        src/utils/Utils.h
        test/benchmarks/queries/queries.h
        test/benchmarks/queries/queryUtils.h
        test/aggregation/windows/windowFunctions/aggregateFunctions.h
        test/aggregation/windows/windowTypes/Windows.h
        )
SET(HEADERS_DATA_STRUCTURES
        )
# common source shared
SET(SRC_WINSUM
        #WinSum/WinSumBase.cpp
        #WinSum/WinSum_addlong.cpp
        )
SET(SOURCES
        ${HEADERS}
        #benchmarks/query.cpp
        #${SRC_WINSUM}
        )

# -- apply to all configurations
#SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -g -fcilkplus -Wall" )
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include utils.h -fdiagnostics-color=auto -D_GLIBCXX_USE_CXX11_ABI=0")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include utils.h -fdiagnostics-color=auto")

#SET(CMAKE_CXX_FLAGS_DEBUG
#        "${CMAKE_CXX_FLAGS_DEBUG} -std=c++17 -g -O0 -Wall -Wfatal-errors") #-DHAVE_NUMA
#SET( CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -std=c++11 -g -fcilkplus -Wall -O2" )
#SET(CMAKE_CXX_FLAGS_RELEASE "-std=c++17 -g -Wall -O3") #-DHAVE_NUMA
#SET(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS} -std=c++11 -g -fcilkplus -Wall")
#if (CMAKE_BUILD_TYPE STREQUAL "Debug")
#    add_definitions(-DCONFIG_KAGE_GLOBAL_DEBUG_LEVEL=30)
#    #	add_definitions(-D_GLIBCXX_DEBUG)	   # will fail compilation in folly
#endif ()
#if (CMAKE_BUILD_TYPE STREQUAL "Release")
#    add_definitions(-DCONFIG_KAGE_GLOBAL_DEBUG_LEVEL=50)
#    add_definitions(-DNDEBUG)
#endif ()
#if (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
#    add_definitions(-DCONFIG_KAGE_GLOBAL_DEBUG_LEVEL=20)
#endif ()

# SET( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -lboost_date_time" )
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")
#SET( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS}" )
#SET( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -pthread -L/home/xzl/tmp/boost_1_61_0/stage/lib -static" )


# xzl: tbb will be in the default lib path
#FIND_LIBRARY(tbb NAMES libtbb.so)
#FIND_LIBRARY(tbb NAMES libtbb.so PATHS tbb/lib/intel64/gcc4.7)
#FIND_LIBRARY(tbb_malloc NAMES libtbbmalloc.so PATHS tbb/lib/intel64/gcc4.7)
#TARGET_LINK_LIBRARIES(test.bin boost_date_time boost_system boost_thread numa ${tbb}))


# Boost Library
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.63.0 COMPONENTS filesystem regex program_options iostreams)


# VTune directory
set(VTUNE_DIR ${VTUNE_HOME}/vtune_amplifier)
#${VTUNE_HOME}/vtune_amplifier)#"/media/george/DATA/intel/vtune_amplifier")
include_directories(${VTUNE_DIR}/include)
#include_directories(/media/george/DATA/intel/vtune_amplifier/lib64)
#link_directories(/media/george/DATA/intel/vtune_amplifier/lib64)
#include_directories(/media/george/DATA/intel/vtune_amplifier/lib64/runtime)
#link_directories(/media/george/DATA/intel/vtune_amplifier/lib64/runtime)

# Locate GTest
#find_package(GTest REQUIRED)
#include_directories(${GTEST_INCLUDE_DIRS})
#target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)

# Locate GBenchmark
#find_package(benchmark REQUIRED)
#include_directories(${benchmark_INCLUDE_DIRS})
#target_link_libraries(runTests ${benchmark_LIBRARIES} pthread)

