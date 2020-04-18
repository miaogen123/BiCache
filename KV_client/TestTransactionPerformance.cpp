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
int transaction_count =0;
void SigIntHandler(int signum){
  info("transaction {}, write count {}, read_count {}", transaction_count, write_count, read_count);
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

  std::vector<std::string> keys;
  std::vector<std::string> values;
  
  static std::default_random_engine gen;
  static std::uniform_int_distribution<int> dis(0,10000); 
  std::vector<uint32_t> operation_ids;
  do{
    values.clear();
    keys.clear();
    auto start = dis(gen);
    //write
    for(auto i= start;i<start+7;i++){
      keys.push_back(std::to_string(i));
      values.push_back(std::to_string(i+5));
    }
    operation_ids.resize(keys.size());

    operation_ids[5]=1;
    operation_ids[6]=1;
    values[5]="";
    values[6]="";
    transaction_count++;
    kvc.Transaction(keys, values, operation_ids);
    auto v5 = std::atoi(values[5].c_str());
    auto v6 = std::atoi(values[6].c_str());
    if(v6!=v5+1){
      critical("data not consistent: ");
    }
    info("two values {}, {}", values[5], values[6]);
    usleep(1000);
  }while(true);
  return 0;
}