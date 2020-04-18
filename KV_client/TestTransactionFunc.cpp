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
  auto test_read_transaction = conf.get("test_read_transaction", "");

  KV_client kvc(conf);
  kvc.GetTology();

  std::vector<std::string> keys{"1", "2", "3"};
  std::vector<std::string> values;
  
  std::vector<uint32_t> ac_values{1, 2, 3};
  do{
    if(test_read_transaction.size()){
      values.clear();
      std::vector<uint32_t> operation_ids(keys.size(), 1);
      values.resize(keys.size());
      kvc.Transaction(keys, values, operation_ids);
      for(auto i=0;i<values.size();i++){
        ac_values[i]=std::atoi(values[i].c_str());
      }
      for(auto i=0;i<ac_values.size()-1;i++){
        if(ac_values[i]+1!=ac_values[i+1]){
          critical("data not consistent: ");
          break;
        }
      }
      info("three values {}, {}, {}", ac_values[0], ac_values[1], ac_values[2]);
    }else{
      values.clear();
      for(auto& value:ac_values){
        value+=2;
        values.push_back(std::to_string(value));
      }
      info("three values {}, {}, {}", ac_values[0], ac_values[1], ac_values[2]);
      std::vector<uint32_t> operation_ids;
      operation_ids.resize(keys.size());
      kvc.Transaction(keys, values, operation_ids);
    }
    usleep(100000);
  }while(true);
  return 0;
}