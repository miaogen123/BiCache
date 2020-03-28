
cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_STANDARD 17)

project(Bicache C CXX)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

###
# include externalproject 
include(../external_modules/CMakeLists.txt)
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

#cmake 这里的构建规则，有精力再完善这里的做法
#set(proto_generated_srcs, {"test"})
#message(list(GET, ${proto_generated_srcs}, 0))
#set(proto_generated_srcs, "a", "b")
#foreach(X
#  "ConsistentHash"
#  "ProxyServer"
#)
#
#get_filename_component(proto_file_path "../proto/{X}.proto" ABSOLUTE)
#get_filename_component(proto_path "${proto_file_path}" PATH)
#
#set(proto_grpc_hdrs, "${CMAKE_CURRENT_BINARY_DIR}/../pb/${X}.grpc.pb.h")
#set(proto_grpc_srcs, "${CMAKE_CURRENT_BINARY_DIR}/../pb/${X}.grpc.pb.cc")
#set(proto_pb_hdrs, "${CMAKE_CURRENT_BINARY_DIR}/../pb/${X}.pb.h")
#set(proto_pb_srcs, "${CMAKE_CURRENT_BINARY_DIR}/../pb/${X}.pb.cc")
#
##list(APPEND,${proto_generated_srcs},${proto_pb_srcs})
##list(APPEND,${proto_generated_srcs},${proto_grpc_srcs})
#add_custom_command(
#      OUTPUT "${proto_grpc_hdrs}" "${proto_grpc_srcs}" "${proto_pb_hdrs}" "${proto_pb_srcs}"
#      COMMAND ${_PROTOBUF_PROTOC}
#      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
#        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/../pb"
#        -I "${proto_file_path}"
#        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
#        "${proto_file_path}"
#      DEPENDS "${proto_file_path}")
#      message("helo")
#endforeach()



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

 get_filename_component(kv_proto "../proto/KV_store.proto" ABSOLUTE)
 get_filename_component(kv_proto_path "${ch_proto}" PATH)
 
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

#set(CMAKE_CXX_FLAGS "-O0")
add_compile_options("-O0")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}/build")
message(STATUS ${CMAKE_CURRENT_BINARY_DIR})

file(GLOB curr_dir_all_cpp "*.cpp")
message(STATUS ${curr_dir_all_cpp})
# Targets greeter_[async_](client|server)
foreach(_target
  KV_store
  )
  add_executable(${_target}
     ${hw_proto_srcs}
     ${hw_grpc_srcs}
     ${ch_proto_srcs}
     ${ch_grpc_srcs}
     ${kv_proto_srcs}
     ${kv_grpc_srcs}
     ${curr_dir_all_cpp}
     ${local_utils}
     ${git_utils})
  target_link_libraries(${_target}
    ${_GRPC_GRPCPP_UNSECURE}
    ${_PROTOBUF_LIBPROTOBUF}
    spdlog::spdlog
    )
endforeach()
