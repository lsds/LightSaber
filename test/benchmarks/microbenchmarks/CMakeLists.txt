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
        ../../../src/dispatcher/TaskDispatcher.cpp
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
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

# Operator Microbenchmarks
add_executable(TestProjection
        TestProjection.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(TestProjection PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestSelection
        TestSelection.cpp
        ${CPP_FILES}
        )
target_link_options(TestSelection PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestSelection ${Boost_LIBRARIES})
endif ()
target_link_libraries(TestSelection
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb
        pthread dl)
target_compile_options(TestSelection PRIVATE -Wall -Wextra -O3 -march=native)

add_executable(TestAggregation
        TestAggregation.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(TestAggregation PRIVATE -Wall -Wextra -O3 -march=native)

# GAG standalone Microbenchmarks
add_executable(TestGAG
        TestGAG.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(TestGAG PRIVATE -Wall -Wextra -O3 -march=native)