include_directories(ClusterMonitoring)
include_directories(LinearRoadBenchmark)
include_directories(SmartGrid)
include_directories(YahooBenchmark)
include_directories(ManufacturingEquipment)


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
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUM -DPREFETCH")

find_package(benchmark REQUIRED)
include_directories(${benchmark_INCLUDE_DIRS})

FIND_LIBRARY(tbb NAMES libtbb.so)

# ManufacturingEquipment
add_executable(manufacturing_equipment
        ManufacturingEquipment/main.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(manufacturing_equipment PRIVATE -Wall -Wextra -O3 -march=native)


# Cluster Monitoring
add_executable(cluster_monitoring
        ClusterMonitoring/main.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(cluster_monitoring PRIVATE -Wall -Wextra -O3 -march=native)


# Linear Road Benchmark
add_executable(linear_road_benchmark
        LinearRoadBenchmark/main.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(linear_road_benchmark PRIVATE -Wall -Wextra -O3 -march=native)


# Yahoo Benchmark
add_executable(yahoo_benchmark
        YahooBenchmark/main.cpp
        ${CPP_FILES}
        )
target_link_options(yahoo_benchmark PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(yahoo_benchmark ${Boost_LIBRARIES})
endif ()
target_link_libraries(yahoo_benchmark
        operatorJITLib
        boost_fiber
        tbb
        pthread dl)
target_compile_options(yahoo_benchmark PRIVATE -Wall -Wextra -O3 -march=native)


# Smart Grid
add_executable(smartgrid
        SmartGrid/main.cpp
        ${CPP_FILES}
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
        tbb
        pthread dl)
target_compile_options(smartgrid PRIVATE -Wall -Wextra -O3 -march=native)
target_link_options(smartgrid PRIVATE -Wl,--unresolved-symbols=ignore-all)
