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
SET(SOURCES
        ${HEADERS}
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

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