#include <iostream>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
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
  //spdlog::set_level(spdlog::level::info);
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S:%e] [%^%L%$] [tid %t] %v");
  //std::unordered_map<std::string, std::string> conf;
  std::string port;
  
  Conf conf("Client_config");
  expire_time = atoi(conf.get("expire_time", "20000").c_str());
  KV_client kvc(conf);
  kvc.GetTology();
  

  std::vector<std::string> keys;
  std::vector<std::string> values;
  for(auto i = 0 ;i<1;i++){
    std::string key = getRandStr(16);
    std::string value = getRandStr(16);
    keys.push_back(key);
    values.push_back(value);
    kvc.Set(key, value);
  }
  for(auto i = 0 ;i<1;i++){
    std::string key = keys[i];
    kvc.Get(key);
    usleep(400000);
  }
  int input_value=0;
  int i = 0;
  do{
    std::string key;
    std::string value;
    if(input_value == 0){
      key = getRandStr(16);
      value = getRandStr(16);
      keys.push_back(key);
      values.push_back(value);
      kvc.Set(key, value);
      i++;
    }else if(input_value == 1){
      key = keys[i];
      kvc.Get(key);
    }else if(input_value == 2  ){
      std::cin>>key;
      kvc.Get(key);
    }else if(input_value == 3){
      std::cin >> key ;
      std::cin >> value ;
      kvc.Set(key, value);
    }else if(input_value == 4){
      //test get
      key = keys[i];
      kvc.Get(key);
    }else if(input_value == 5){
      //transaction
      std::vector<std::string> keys;
      std::vector<std::string> values;
      std::string keys_one_line;
      std::string values_one_line;

      //这玩意有点坑啊
      std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n' );
      std::getline(std::cin,keys_one_line, '\n');
      std::getline(std::cin,values_one_line, '\n');

      SplitString(keys_one_line, keys, ' ');
      SplitString(values_one_line, values, ' ');
      kvc.Transaction(keys, values);
    }
    if(input_value !=4){
      std::cin>>input_value;
      usleep(1000);
    }
  }while(input_value!=9);
  return 0;
}