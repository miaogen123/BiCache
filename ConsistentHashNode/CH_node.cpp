
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "../utils/conf.h"
#include "../utils/host.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "KV_store.h"

#define SPDLOG_DEBUG_ON
void start_kv_service(Conf& conf){
  auto KV_port= conf.get("KV_port");
  if(KV_port.size()==0){
    spdlog::critical("get KV_node_port error");
    exit(-1);
  }
  std::string server_address("0.0.0.0:"+KV_port);
  auto kv_store_impl = std::make_shared<KV_store_impl>(conf);
  //CH_node_impl node{inner_conf};
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(kv_store_impl.get());
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("KV service start at {}", server_address);
  kv_store_impl->init();
  server->Wait();
}

int main() {
  //spdlog::set_level(spdlog::level::info);
//  auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
//  std::stringstream ss;
//	ss << std::put_time(std::localtime(&t), "%Y-%m-%d_%H:%M:%S.log");
//	std::string str = ss.str();
//  spdlog::set_default_logger(std::make_shared<spdlog::>("file_logger", "KV"+str, 1024*1024*1024, 100));
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S:%e] [%^%L%$] [tid %t] %v");
  Conf conf("CH_config");
  if(!conf.get_file_stated()){
    spdlog::critical("can't find conf file");
    return -1;
  }

  std::string host_name;
  std::string ip;
  int ret = get_host_info(host_name, ip);
  if(ret){
    spdlog::critical("get hostip error");
    exit(-1);
  }
  conf.set("host_ip", ip);
  start_kv_service(conf);
  return 0;
}