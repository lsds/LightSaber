set(CMAKE_CXX_STANDARD 17)

# Configure CCache if available
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    message("Using CCache...")
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    #set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_C_COMPILER_LAUNCHER   ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER "/usr/lib/ccache/clang++")
endif()

if (CMAKE_BUILD_TYPE MATCHES "^[Rr]elease")
    # first we can indicate the documentation build as an option and set it to ON by default
    option(BUILD_DOC "Build documentation" ON)

    # check if Doxygen is installed
    find_package(Doxygen)
    if (DOXYGEN_FOUND)
        # set input and output files
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/../docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

        # request to configure the file
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        message("Doxygen build started")

        # note the option ALL which allows to build the docs together with the application
        add_custom_target( doc ALL
                COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                COMMENT "Generating API documentation with Doxygen"
                VERBATIM )

    else (DOXYGEN_FOUND)
        message("Doxygen need to be installed to generate the doxygen documentation")
    endif (DOXYGEN_FOUND)
endif()

add_library(operatorJITLib SHARED
        cql/operators/codeGeneration/OperatorJit.cpp)
set_target_properties(operatorJITLib PROPERTIES PUBLIC_HEADER cql/operators/codeGeneration/OperatorJit.h)
#set_target_properties(operatorJITLib PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(operatorJITLib PRIVATE -Wall -Wextra -O3 -march=native)

SET(HEADERS
        utils/Utils.h
        buffers/NUMACircularQueryBuffer.h
        buffers/CircularQueryBuffer.h
        buffers/QueryBuffer.h
        buffers/PartialWindowResults.h
        buffers/PartialWindowResultsFactory.h
        buffers/UnboundedQueryBuffer.h
        buffers/UnboundedQueryBufferFactory.h
        cql/expressions/Expression.h
        cql/expressions/operations/Division.h
        cql/expressions/operations/Addition.h
        cql/expressions/ColumnReference.h
        cql/expressions/operations/Multiplication.h
        cql/expressions/operations/Subtraction.h
        cql/expressions/IntConstant.h
        cql/expressions/FloatConstant.h
        cql/expressions/LongConstant.h
        cql/operators/HashFunctions.h
        cql/operators/OperatorCode.h
        cql/operators/AggregateOperatorCode.h
        cql/operators/AggregationType.h
        cql/operators/NoOp.h
        cql/operators/Aggregation.h
        cql/operators/Projection.h
        cql/operators/Selection.h
        cql/operators/codeGeneration/OperatorKernel.h
        cql/predicates/Predicate.h
        cql/predicates/ComparisonPredicate.h
        cql/predicates/ANDPredicate.h
        cql/predicates/ORPredicate.h
        dispatcher/TaskDispatcher.h
        monitors/LatencyMonitor.h
        monitors/Measurement.h
        monitors/PerformanceMonitor.h
        processor/TaskProcessor.h
        processor/TaskProcessorPool.h
        result/ResultHandler.h
        tasks/Task.h
        tasks/TaskFactory.h
        tasks/WindowBatch.h
        tasks/WindowBatchFactory.h
        utils/AttributeType.h
        utils/PaddedLong.h
        utils/Query.h
        utils/QueryApplication.h
        utils/QueryOperator.h
        utils/SystemConf.h
        utils/TupleSchema.h
        utils/WindowDefinition.h
        )
SET(CPP_FILES
        dispatcher/TaskDispatcher.cpp
        monitors/PerformanceMonitor.cpp
        monitors/LatencyMonitor.cpp
        monitors/Measurement.cpp
        processor/TaskProcessor.cpp
        result/ResultHandler.cpp
        tasks/WindowBatch.cpp
        tasks/NumaTaskQueueWrapper.cpp
        tasks/Task.cpp
        utils/AttributeType.cpp
        utils/Query.cpp
        utils/QueryApplication.cpp
        utils/SystemConf.cpp
        utils/Utils.cpp
        )

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lnuma")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++17 -g -Wall -Wextra -DHAVE_NUMA" )

#-mcx16

add_executable(LightSaber ${HEADERS} ${CPP_FILES} main.cpp)
target_compile_options(LightSaber PRIVATE -O3 -march=native)


#SET(DISABLE_RTTI_FLAG -fno-rtti)

message("")
message("LLVM & Clang Dependencies")

if (WIN32)
    set(OS_NAME "Win")
    set(DISABLE_RTTI_FLAG /GR-)
elseif (APPLE)
    set(OS_NAME "OSX")
    set(DISABLE_RTTI_FLAG -fno-rtti)
elseif (UNIX)
    set(OS_NAME "Linux")
    set(DISABLE_RTTI_FLAG -fno-rtti)
else ()
    message(FATAL_ERROR "Operating system not supported")
endif ()
message(STATUS "System: ${OS_NAME}")

set_target_properties(operatorJITLib PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        )

# add LLVM libraries, set LLVM_* variables
find_package(LLVM 9.0 REQUIRED PATHS ${LLVM_DIR})
message(STATUS "LLVM_DIR: ${LLVM_DIR}")
message(STATUS "LLVM_PACKAGE_VERSION: ${LLVM_PACKAGE_VERSION}")

# add Clang libraries
include(${LLVM_BUILD_BINARY_DIR}/lib/cmake/clang/ClangTargets.cmake)

#set(LLVM_ENABLE_RTTI ON)
if (NOT LLVM_ENABLE_RTTI)
    target_compile_options(operatorJITLib PRIVATE ${DISABLE_RTTI_FLAG})
endif ()

if (USE_LLD)
    message(STATUS "Add option to link with LLD")
    target_link_libraries(operatorJITLib PRIVATE "-fuse-ld=lld")
endif ()

# find Clang source directory
if (EXISTS ${LLVM_BUILD_MAIN_SRC_DIR}/tools/clang)
    set(CLANG_SRC_DIR_PREFIX tools) # sources in-tree (default build)
elseif (EXISTS ${LLVM_BUILD_MAIN_SRC_DIR}/../clang)
    set(CLANG_SRC_DIR_PREFIX ..) # sources out-of-tree (ENABLE_PROJECTS build)
else ()
    message(FATAL_ERROR "Cannot find Clang sources")
endif ()

message(STATUS "LLVM Source Directory: ${LLVM_BUILD_MAIN_SRC_DIR}")
message(STATUS "Clang Source Directory: ${LLVM_BUILD_MAIN_SRC_DIR}/${CLANG_SRC_DIR_PREFIX}/clang")
message(STATUS "LLVM/Clang Build Directory: ${LLVM_BUILD_BINARY_DIR}")

target_include_directories(operatorJITLib PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${LLVM_INCLUDE_DIRS}
        ${LLVM_BUILD_BINARY_DIR}/tools/clang/include
        ${LLVM_BUILD_MAIN_SRC_DIR}/${CLANG_SRC_DIR_PREFIX}/clang/include
        )
# LLVM definitions
separate_arguments(LLVM_DEFINITIONS)
target_compile_definitions(operatorJITLib PRIVATE
        ${LLVM_DEFINITIONS}
        )

# operatorJITLib dependencies
#  llvm_map_components_to_libnames(LLVM_LIBS
#         irreader
#         Analysis
#         Core
#         ExecutionEngine
#         InstCombine
#         #MC
#         #MCJIT
#         Object
#         Option
#         OrcJIT
#         RuntimeDyld
#         ScalarOpts
#         Support
#         native
#         )

llvm_config(operatorJITLib support core
          analysis
        executionengine
        instcombine
        #mc
        #mcjit
        object
        option
        OrcJIT
        target
        ExecutionEngine
        runtimedyld
        scalaropts
        native
)
# clang_config(operatorJITLib Basic)
#       message("===" ${LLVM_LIBS})
# target_link_libraries(operatorJITLib PRIVATE
#         ${LLVM_LIBS}
#         )
# Project-specific definitions
target_compile_definitions(operatorJITLib
        PRIVATE
        # Supply path to Clang resources in the LLVM/Clang build directory
        OPERATOR_JIT_LIB_CLANG_RESOURCE_DIR=${LLVM_BUILD_BINARY_DIR}/lib/clang/9.0.0#${LLVM_PACKAGE_VERSION}
        )

# Clang dependencies
target_link_libraries(operatorJITLib PRIVATE
        clang# Basic
        clangCodeGen
        # clangDriver
        # clangFrontend
        # clangSerialization

        #LLVMADT
        # LLVMExecutionEngine
        #LLVMIR
        # LLVMOrcJIT
        # LLVMSupport
        # LLVMTarget
        )

message("")
message("operatorJITLib")

get_target_property(ALL_INCLUDE_DIRECTORIES operatorJITLib INCLUDE_DIRECTORIES)
message(STATUS "Include directories: ${ALL_INCLUDE_DIRECTORIES}")

get_target_property(ALL_LINK_LIBRARIES operatorJITLib LINK_LIBRARIES)
message(STATUS "Link libraries: ${ALL_LINK_LIBRARIES}")

get_target_property(ALL_COMPILE_OPTIONS operatorJITLib COMPILE_OPTIONS)
message(STATUS "Compile options: ${ALL_COMPILE_OPTIONS}")

get_target_property(ALL_COMPILE_DEFINITIONS operatorJITLib COMPILE_DEFINITIONS)
message(STATUS "Compile definitions: ${ALL_COMPILE_DEFINITIONS}")

message(STATUS "Other flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "Other flags Debug: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "Other flags Release: ${CMAKE_CXX_FLAGS_RELEASE}")

# ...
function(export_executable_symbols target)
    if (LLVM_EXPORTED_SYMBOL_FILE)
        # The symbol file should contain the symbols we want the executable to
        # export
        set_target_properties(${target} PROPERTIES ENABLE_EXPORTS 1)
    elseif (LLVM_EXPORT_SYMBOLS_FOR_PLUGINS)
        # Extract the symbols to export from the static libraries that the
        # executable links against.
        set_target_properties(${target} PROPERTIES ENABLE_EXPORTS 1)
        set(exported_symbol_file ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/${target}.symbols)
        # We need to consider not just the direct link dependencies, but also the
        # transitive link dependencies. Do this by starting with the set of direct
        # dependencies, then the dependencies of those dependencies, and so on.
        get_target_property(new_libs ${target} LINK_LIBRARIES)
        set(link_libs ${new_libs})
        while (NOT "${new_libs}" STREQUAL "")
            foreach (lib ${new_libs})
                if (TARGET ${lib})
                    get_target_property(lib_type ${lib} TYPE)
                    if ("${lib_type}" STREQUAL "STATIC_LIBRARY")
                        list(APPEND static_libs ${lib})
                    else ()
                        list(APPEND other_libs ${lib})
                    endif ()
                    get_target_property(transitive_libs ${lib} INTERFACE_LINK_LIBRARIES)
                    foreach (transitive_lib ${transitive_libs})
                        list(FIND link_libs ${transitive_lib} idx)
                        if (TARGET ${transitive_lib} AND idx EQUAL -1)
                            list(APPEND newer_libs ${transitive_lib})
                            list(APPEND link_libs ${transitive_lib})
                        endif ()
                    endforeach (transitive_lib)
                endif ()
            endforeach (lib)
            set(new_libs ${newer_libs})
            set(newer_libs "")
        endwhile ()
        if (MSVC)
            set(mangling microsoft)
        else ()
            set(mangling itanium)
        endif ()
        add_custom_command(OUTPUT ${exported_symbol_file}
                COMMAND ${PYTHON_EXECUTABLE} ${LLVM_MAIN_SRC_DIR}/utils/extract_symbols.py --mangling=${mangling} ${static_libs} -o ${exported_symbol_file}
                WORKING_DIRECTORY ${LLVM_LIBRARY_OUTPUT_INTDIR}
                DEPENDS ${LLVM_MAIN_SRC_DIR}/utils/extract_symbols.py ${static_libs}
                VERBATIM
                COMMENT "Generating export list for ${target}")
        add_llvm_symbol_exports(${target} ${exported_symbol_file})
        # If something links against this executable then we want a
        # transitive link against only the libraries whose symbols
        # we aren't exporting.
        set_target_properties(${target} PROPERTIES INTERFACE_LINK_LIBRARIES "${other_libs}")
        # The default import library suffix that cmake uses for cygwin/mingw is
        # ".dll.a", but for clang.exe that causes a collision with libclang.dll,
        # where the import libraries of both get named libclang.dll.a. Use a suffix
        # of ".exe.a" to avoid this.
        if (CYGWIN OR MINGW)
            set_target_properties(${target} PROPERTIES IMPORT_SUFFIX ".exe.a")
        endif ()
    elseif (NOT (WIN32 OR CYGWIN))
        # On Windows auto-exporting everything doesn't work because of the limit on
        # the size of the exported symbol table, but on other platforms we can do
        # it without any trouble.
        set_target_properties(${target} PROPERTIES ENABLE_EXPORTS 1)
        if (APPLE)
            set_property(TARGET ${target} APPEND_STRING PROPERTY
                    LINK_FLAGS " -rdynamic")
        endif ()
    endif ()
endfunction()
export_executable_symbols(operatorJITLib)

if (MSVC)
    # Is this a CMake bug that even with export_executable_symbols, Windows
    # needs to explictly export the type_info vtable
    set_property(TARGET operatorJITLib
            APPEND_STRING PROPERTY LINK_FLAGS " /EXPORT:??_7type_info@@6B@")
endif ()

function(clang_enable_exceptions TARGET)
    # Really have to jump through hoops to enable exception handling independent
    # of how LLVM is being built.
    if (NOT LLVM_REQUIRES_EH AND NOT LLVM_REQUIRES_RTTI)
        if (MSVC)
            # /EHs to allow throwing from extern "C"
            set(excptnExceptions_ON "/D _HAS_EXCEPTIONS=1 /EHs /wd4714")
            set(excptnExceptions_OFF "/D _HAS_EXCEPTIONS=0 /EHs-c-")
            set(excptnRTTI_ON "/GR")
            set(excptnRTTI_OFF "/GR-")
            set(excptnEHRTTIRegEx "(/EHs(-c-?)|_HAS_EXCEPTIONS=(0|1))")
        else ()
            set(excptnExceptions_ON "-fexceptions")
            set(excptnExceptions_OFF "-fno-exceptions")
            set(excptnRTTI_ON "-frtti")
            set(excptnRTTI_OFF "-fno-rtti")
            set(excptnEHRTTIRegEx "-f(exceptions|no-exceptions)")
        endif ()
        if (LLVM_REQUIRES_EH)
            set(excptnExceptions_DFLT ${excptnExceptions_ON})
        else ()
            set(excptnExceptions_DFLT ${excptnExceptions_OFF})
        endif ()
        if (LLVM_REQUIRES_RTTI)
            set(excptnRTTI_DFLT ${excptnRTTI_ON})
        else ()
            set(excptnRTTI_DFLT ${excptnRTTI_OFF})
        endif ()

        # Strip the exception & rtti flags from the target
        get_property(addedFlags TARGET ${TARGET} PROPERTY COMPILE_FLAGS)
        string(REGEX REPLACE ${excptnEHRTTIRegEx} "" editedFlags "${addedFlags}")
        string(REPLACE ${excptnRTTI_OFF} "" editedFlags "${editedFlags}")
        set_property(TARGET ${TARGET} PROPERTY COMPILE_FLAGS "${editedFlags}")

        get_property(addedFlags TARGET ${TARGET} PROPERTY COMPILE_DEFINITIONS)
        string(REGEX REPLACE ${excptnEHRTTIRegEx} "" editedFlags "${addedFlags}")
        string(REPLACE ${excptnRTTI_OFF} "" editedFlags "${editedFlags}")
        set_property(TARGET ${TARGET} PROPERTY COMPILE_DEFINITIONS "${editedFlags}")

        # Re-add the exception & rtti flags from LLVM
        set_property(SOURCE cql/operators/codeGeneration/OperatorJit.cpp APPEND_STRING PROPERTY COMPILE_FLAGS
                " ${excptnExceptions_DFLT} ${excptnRTTI_DFLT} ")
        set_property(SOURCE Manager.cpp APPEND_STRING PROPERTY COMPILE_FLAGS
                " ${excptnExceptions_DFLT} ${excptnRTTI_DFLT} ")

        # Invoke with exceptions & rtti
        set_property(SOURCE Invoke.cpp APPEND_STRING PROPERTY COMPILE_FLAGS
                " ${excptnExceptions_ON} ${excptnRTTI_ON} ")

    endif ()
endfunction(clang_enable_exceptions)

clang_enable_exceptions(operatorJITLib)

target_link_libraries(LightSaber PRIVATE
        operatorJITLib
        boost_fiber
        )
target_link_options(LightSaber PRIVATE -Wl,--unresolved-symbols=ignore-all)

#set(TBB_DIR ${VTUNE_HOME}/tbb)
#${VTUNE_HOME}/vtune_amplifier)#"/media/george/DATA/intel/vtune_amplifier") /home/grt17/intel/tbb/lib/intel64_lin
#include_directories(${TBB_DIR}/include/tbb)
#include_directories(${TBB_DIR}/lib/intel64_lin/gcc4.7/)

FIND_LIBRARY(tbb NAMES libtbb.so)
#FIND_LIBRARY(tbb NAMES libtbb.so PATHS tbb/lib/intel64/gcc4.7)
#FIND_LIBRARY(tbb_malloc NAMES libtbbmalloc.so PATHS tbb/lib/intel64/gcc4.7)

target_link_libraries(LightSaber PRIVATE tbb)
target_link_libraries(LightSaber PRIVATE dl)


if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(LightSaber PRIVATE
            ${Boost_LIBRARIES})
endif ()


#target_link_libraries(LightSaber PRIVATE pthread)
#target_link_libraries(LightSaber PRIVATE atomic)
