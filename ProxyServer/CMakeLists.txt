# Copyright 2018 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cmake build file for C++ helloworld example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building helloworld.

cmake_minimum_required(VERSION 3.1)

project(Bicache C CXX)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

###
# include externalproject 
include(../external_modules/CMakeLists.txt)
#include(../spdlog/cmake/utils.cmake)
#
if(NOT TARGET spdlog)
    # Stand-alone build
    find_package(spdlog REQUIRED)
endif()


# This branch assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

include_directories(../MyUtils/cpp)
# message(STATUS ${utils})

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP_UNSECURE gRPC::grpc++_unsecure)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# Proto file
get_filename_component(hw_proto "../proto/ProxyServer.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
get_filename_component(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ProxyServer.pb.cc" ABSOLUTE)
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ProxyServer.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ProxyServer.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ProxyServer.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

get_filename_component(kv_proto "../proto/KV_store.proto" ABSOLUTE)
get_filename_component(kv_proto_path "${kv_proto}" PATH)

# Generated sources
get_filename_component(kv_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/KV_store.pb.cc" ABSOLUTE)
set(kv_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/KV_store.pb.h")
set(kv_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/KV_store.grpc.pb.cc")
set(kv_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/KV_store.grpc.pb.h")
add_custom_command(
      OUTPUT "${kv_proto_srcs}" "${kv_proto_hdrs}" "${kv_grpc_srcs}" "${kv_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        -I "${kv_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${kv_proto}"
      DEPENDS "${kv_proto}")

# Include generated *.pb.h files

set(git_utils "../MyUtils/cpp/fileOp.cpp" "../MyUtils/cpp/hash.cpp")
file(GLOB local_utils "../utils/*.cpp")

set(target_files "ProxyServer.cpp" "ProxyImpl.cpp")
# Targets greeter_[async_](client|server)
foreach(_target
  ProxyServer
  )
  add_executable(${_target} ${target_files} ${git_utils}
    ${hw_proto_srcs}
    ${hw_grpc_srcs} 
    ${kv_proto_srcs}
    ${kv_grpc_srcs} 
    ${local_utils}
    )
  target_link_libraries(${_target}
    ${_GRPC_GRPCPP_UNSECURE}
    ${_PROTOBUF_LIBPROTOBUF}
    spdlog::spdlog
    )
endforeach()
