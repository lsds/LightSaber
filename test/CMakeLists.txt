project(${CMAKE_PROJECT_NAME}_tests)

include_directories(benchmarks)
include_directories(unit_tests)

add_subdirectory(benchmarks)
add_subdirectory(unit_tests)

# Clone Google Test
#set(GOOGLETEST_DIR ${CMAKE_SOURCE_DIR}/test/lib/googletest)
#if(NOT EXISTS ${GOOGLETEST_DIR})
#    find_package(Git REQUIRED)
#    execute_process(
#            COMMAND ${GIT_EXECUTABLE} clone https://github.com/google/googletest ${GOOGLETEST_DIR}
#    )
#endif()
#add_subdirectory(lib/googletest)



