
cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_STANDARD 14)

project(Bicache C CXX)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

###
# include externalproject 
include(../external_modules/CMakeLists.txt)


# This branch assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")
file(GLOB git_utils "../MyUtils/cpp/*.cpp")
file(GLOB local_utils "../utils/*.cpp")

include_directories(../MyUtils/cpp)

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP_UNSECURE gRPC::grpc++_unsecure)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

#ProxyServer client
get_filename_component(hw_proto "../proto/ProxyServer.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

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


#consistent hash interface
get_filename_component(ch_proto "../proto/ConsistentHash.proto" ABSOLUTE)
get_filename_component(ch_proto_path "${ch_proto}" PATH)

get_filename_component(ch_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ConsistentHash.pb.cc" ABSOLUTE)
set(ch_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ConsistentHash.pb.h")
set(ch_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ConsistentHash.grpc.pb.cc")
set(ch_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/../pb/ConsistentHash.grpc.pb.h")
add_custom_command(
      OUTPUT "${ch_proto_srcs}" "${ch_proto_hdrs}" "${ch_grpc_srcs}" "${ch_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
        -I "${ch_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${ch_proto}"
      DEPENDS "${ch_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}/build")
message(STATUS ${CMAKE_CURRENT_BINARY_DIR})

file(GLOB curr_dir_all_cpp "*.cpp")
message(STATUS ${curr_dir_all_cpp})
# Targets greeter_[async_](client|server)
foreach(_target
  CH_node
  )
  add_executable(${_target}
     ${hw_proto_srcs}
     ${hw_grpc_srcs}
     ${ch_proto_srcs}
     ${ch_grpc_srcs}
     ${curr_dir_all_cpp}
     ${local_utils}
     ${git_utils})
  target_link_libraries(${_target}
    ${_GRPC_GRPCPP_UNSECURE}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()
