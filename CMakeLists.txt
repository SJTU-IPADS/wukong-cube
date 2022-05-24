## Set root directory of Wukong
set(ROOT $ENV{WUKONG_ROOT})
set(CMAKE_CXX_COMPILER ${ROOT}/deps/openmpi-1.6.5-install/bin/mpic++)

project (wukong)

## CMake version
cmake_minimum_required(VERSION 2.8)

## Use C++17 features
add_definitions(-std=c++17)


## Set dependencies
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -fopenmp -Wno-literal-suffix")
set(BOOST_LIB_DIR "${ROOT}/deps/boost_1_67_0-install/lib")
set(WUKONG_LIBS ${WUKONG_LIBS} zmq rt tbb hwloc)
set(BOOST_LIBS ${BOOST_LIBS} ${BOOST_LIB_DIR}/libboost_mpi.so ${BOOST_LIB_DIR}/libboost_serialization.so ${BOOST_LIB_DIR}/libboost_program_options.so ${BOOST_LIB_DIR}/libboost_thread.so ${BOOST_LIB_DIR}/libboost_system.so)

## Set include paths
include_directories(deps/boost_1_67_0-install/include)
include_directories(src)
include_directories(deps)
include_directories(deps/eigen)
include_directories(deps/nlohmann/include)
include_directories(deps/progress-cpp/include)
include_directories(deps/googletest/googletest/include)
include_directories(deps/rpclib)

## Add sub-directories
add_subdirectory(${ROOT}/deps/googletest)
add_subdirectory(${ROOT}/deps/pybind11)


## Source code
file(GLOB SOURCES "${PROJECT_SOURCE_DIR}/src/*.hpp")
file(GLOB_RECURSE PYTHON_API_SOURCES "${PROJECT_SOURCE_DIR}/src/api/python/*.cpp")
file(GLOB_RECURSE JAVA_API_SOURCES "${PROJECT_SOURCE_DIR}/src/api/java/*.cpp")


## Set options (cached)
## usage: cmake .. -DUSE_RDMA=OFF -DUSE_HADOOP=ON

#### RDMA
option (USE_RDMA "enable RDMA support" ON)
if(USE_RDMA)
  add_definitions(-DHAS_RDMA)
  set(WUKONG_LIBS ${WUKONG_LIBS} ibverbs)
endif(USE_RDMA)

#### GPU (NOT YET READY)
option (USE_GPU "enable GPU support" OFF)
if(USE_GPU)
  add_definitions(-DUSE_GPU)
  find_package(CUDA REQUIRED)

  ## CUDA configs
  set(CUDA_NVCC_FLAGS -arch=sm_35;-std=c++11)
  set(CUDA_HOST_COMPILER ${CMAKE_CXX_COMPILER})
  set(CUDA_SEPARABLE_COMPILATION ON)
  set(WUKONG_LIBS ${WUKONG_LIBS} cudart)
  cuda_add_executable(wukong ${SOURCES} "src/wukong.cpp" "src/gpu/gpu_hash.cu")

else(USE_GPU)
  add_executable(wukong ${SOURCES} "src/wukong.cpp")

endif(USE_GPU)

#### HDFS
option (USE_HADOOP "enable HDFS support" OFF)
if(USE_HADOOP)
  add_definitions(-DHAS_HADOOP)
  target_link_libraries(wukong hdfs)
  set(WUKONG_LIBS ${WUKONG_LIBS} hdfs)
endif(USE_HADOOP)

#### JEMALLOC
## Currently, all malloc APIs(e.g. malloc/free, new/delete) are jemalloc versions by default.
## To change it, go to deps/deps.sh, modify configuration.
target_link_libraries(wukong jemalloc)
set(WUKONG_LIBS ${WUKONG_LIBS} jemalloc)

option (USE_JEMALLOC "enable jemalloc support" ON)
if(USE_JEMALLOC)
  add_definitions(-DUSE_JEMALLOC)
endif(USE_JEMALLOC)

#### Dynamic GStore
option (USE_DYNAMIC_GSTORE "enable dynamic gstore" OFF)
if(USE_DYNAMIC_GSTORE)
  add_definitions(-DDYNAMIC_GSTORE)
endif(USE_DYNAMIC_GSTORE)

#### Verstile queries (e.g., ?S ?P ?O)
option (USE_VERSATILE "support versatile queries" ON)
if(USE_VERSATILE)
  add_definitions(-DVERSATILE)
endif(USE_VERSATILE)

#### Support t-sparql
option (TRDF_MODE "support t-sparql queries" OFF)
if(TRDF_MODE)
  add_definitions(-DTRDF_MODE)
endif(TRDF_MODE)

#### Support yacc & bison for sparql
execute_process(
  COMMAND bison -t -rall ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql.y -d -b ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql
)
if(TRDF_MODE)
  execute_process(
    COMMAND flex -o ${PROJECT_SOURCE_DIR}/src/core/sparql/lex.yy.c ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql_time.l
  )
else()
  execute_process(
    COMMAND flex -o ${PROJECT_SOURCE_DIR}/src/core/sparql/lex.yy.c ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql.l
  )
endif(TRDF_MODE)

SET_SOURCE_FILES_PROPERTIES(${PROJECT_SOURCE_DIR}/src/core/sparql/lex.yy.c 
  ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql.tab.c 
  PROPERTIES LANGUAGE CXX)

add_library(sparql_lex 
  ${PROJECT_SOURCE_DIR}/src/core/sparql/lex.yy.c 
  ${PROJECT_SOURCE_DIR}/src/core/sparql/sparql.tab.c)

set(WUKONG_LIBS ${WUKONG_LIBS} sparql_lex)

#### 64-bit ID (32-bit ID by default)
option (USE_DTYPE_64BIT "use 64-bit ID" OFF)
if(USE_DTYPE_64BIT)
  add_definitions(-DDTYPE_64BIT)
endif(USE_DTYPE_64BIT)

#### Build java library
option (BUILD_JNI "build java api library" OFF)
if(BUILD_JNI)
  find_package(JNI)
  if (JNI_FOUND)
      message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
      message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
  else ()
      message (FATAL_ERROR "JNI is not found on your machine")
  endif()

  # build libwukong_java.so
  add_library(wukong_java SHARED ${JAVA_API_SOURCES})
  target_include_directories(
      wukong_java
      PRIVATE
      ${JNI_INCLUDE_DIRS}
  )
  target_link_libraries(wukong_java
    ${BOOST_LIBS}
  )

  # build wukong-api.jar
  find_package(Java REQUIRED)
  include(UseJava)
  set(CMAKE_JAVA_COMPILE_FLAGS "-source" "1.8" "-target" "1.8")
  file(GLOB_RECURSE WUKONG_JAVA_FILES "${PROJECT_SOURCE_DIR}/java/src/main/java/com/*.java")
  add_jar(wukong-api ${WUKONG_JAVA_FILES} VERSION 1.0.0-alpha)
endif(BUILD_JNI)

## Build Wukong
target_link_libraries(wukong ${WUKONG_LIBS} ${BOOST_LIBS})

## RPC client demo
add_executable(test_client ${SOURCES} "src/client/test_client.cpp")
target_link_libraries(test_client ${WUKONG_LIBS} ${BOOST_LIBS})

## String server
add_executable(string_server ${SOURCES} "src/stringserver/run_string_server.cpp")
target_link_libraries(string_server ${WUKONG_LIBS} ${BOOST_LIBS})
## String server client demo
add_executable(test_string_client ${SOURCES} "src/stringserver/test_string_client.cpp")
target_link_libraries(test_string_client ${WUKONG_LIBS} ${BOOST_LIBS})


file(GLOB TS  "${ROOT}/tests/test_sample.cc")
add_executable(coretest ${TS})
target_link_libraries(coretest gtest gtest_main ${WUKONG_LIBS} ${BOOST_LIBS})

## tests
enable_testing()

add_test(NAME test COMMAND coretest)
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose DEPENDS coretest)

## code format
file(GLOB_RECURSE FILES_NEED_FORMAT "src/*.cpp" "src/*.cc" "src/*.hpp" "src/*.h"
                                    "test/*.cc"
)
file(GLOB_RECURSE FILES_NEED_LINT "src/*.cpp" "src/*.cc" "src/*.hpp" "src/*.h"
                                  "test/*.cc"
)

add_custom_target(clformat
        COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
        COMMENT "Running clang-format."
        VERBATIM)

add_custom_target(wukong_cpplint
        COMMAND ${PROJECT_SOURCE_DIR}/scripts/cpplint.py ${FILES_NEED_LINT}
        COMMENT "Running cpplint check."
        VERBATIM)

## build docs
find_program(doxygen_EXECUTABLE doxygen NO_CMAKE_SYSTEM_PATH)
find_program(sphinx_build_EXECUTABLE sphinx-build NO_CMAKE_SYSTEM_PATH)
if(doxygen_EXECUTABLE AND sphinx_build_EXECUTABLE)
    add_custom_target(wukong_docs
        COMMAND ${CMAKE_COMMAND} -E make_directory _build
        COMMAND ${doxygen_EXECUTABLE}
        COMMAND ${sphinx_build_EXECUTABLE} . _build/html
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/docs
        VERBATIM
    )
else()
    if(NOT doxygen_EXECUTABLE)
        message(STATUS "Cannot find the doxygen executable.")
    endif()
    if(NOT sphinx_build_EXECUTABLE)
        message(STATUS "Cannot find the sphinx-build executable.")
    endif()
endif()

pybind11_add_module(WukongGraph  ${PYTHON_API_SOURCES})
target_link_libraries(WukongGraph zmq rt tbb hwloc ${BOOST_LIBS}) 