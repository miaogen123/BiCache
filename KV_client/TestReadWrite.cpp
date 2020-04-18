#include <iostream>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
#include <cstdio>
#include <limits>
#include <random>
#include <thread>
#include <shared_mutex>
#include <unistd.h>
#include <cstdio>

#include <grpcpp/grpcpp.h>

#include "../utils/conf.h"
#include "../MyUtils/cpp/hash.h"
#include "../MyUtils/cpp/myutils.h"
#include "../MyUtils/cpp/fileOp.h"
#include "spdlog/spdlog.h"
#include "KV_client.h"

#define SPDLOG_DEBUG_ON

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;
using spdlog::info;
using spdlog::critical;
using spdlog::debug;
using spdlog::warn;

extern uint32_t expire_time;
int write_count =0;
int read_count =0;
void SigIntHandler(int signum){
  info("write count {}, read_count {}", write_count, read_count);
  exit(0);
}

int main(int argc, char** argv) {
  signal(SIGINT, SigIntHandler);
  spdlog::set_level(spdlog::level::info);
  //spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S:%e] [%^%L%$] [tid %t] %v");
  //std::unordered_map<std::string, std::string> conf;
  std::string port;
  
  Conf conf("Client_config");
  expire_time = atoi(conf.get("expire_time", "20000").c_str());
  auto read_limit = atoi(conf.get("read_limit", "10").c_str());

  KV_client kvc(conf);
  kvc.GetTology();
  

  std::vector<std::string> keys;
  std::vector<std::string> values;

  static std::default_random_engine gen;
  static std::uniform_int_distribution<int> dis(0,100); 
  do{
    auto num = dis(gen);
    if(num<read_limit&&write_count>0){
      //read
      //half hit 
      auto half= dis(gen);
      if(half<50){
        //hit
        int index = half*keys.size()/50;
        auto value = kvc.Get(keys[index]);
        if(value!=values[index]){
          critical("err {} ret {} origin {} ", keys[index], value, values[index]);         
        }
        info("hit ");
      }else{
        std::string key = getRandStr(16);
        kvc.Get(key);
        info("nohit ");
      }
      read_count++;
    }else{
      //write
      std::string key = getRandStr(16);
      std::string value = getRandStr(16);
      keys.push_back(key);
      values.push_back(value);
      kvc.Set(key, value);
      info("write");
      write_count++;
    }
    usleep(1000);
  }while(true);
  return 0;
}