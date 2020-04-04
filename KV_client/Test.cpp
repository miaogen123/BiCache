#include <iostream>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <cstdio>

#include <grpcpp/grpcpp.h>

#include "../utils/conf.h"
#include "../MyUtils/cpp/hash.h"
#include "../MyUtils/cpp/myutils.h"
#include "../MyUtils/cpp/fileOp.h"
#include "../pb/ProxyServer.grpc.pb.h"
#include "../pb/KV_store.grpc.pb.h"
#include "spdlog/spdlog.h"

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

uint32_t expire_time = 0;

uint64_t get_miliseconds(){
    using namespace std::chrono;
    steady_clock::duration d;
    d = steady_clock::now().time_since_epoch();
    return duration_cast<milliseconds>(d).count();
}

class KV_client{
public: 
  using PosClientType = std::unordered_map<uint32_t, std::shared_ptr<Bicache::KV_service::Stub>>;
  KV_client(Conf conf){
    auto proxy_ip = conf.get("proxy_ip");
    auto proxy_port = conf.get("proxy_port");
    proxy_ip_port_ = proxy_ip+":"+proxy_port;
    proxy_client_ = std::make_shared<Bicache::ProxyServer::Stub>(grpc::CreateChannel(
        proxy_ip_port_, grpc::InsecureChannelCredentials() ));
  }

  void GetTology(){
    ClientContext ctx;
    Bicache::GetConfigRequest req;
    Bicache::GetConfigReply rsp;
    auto status = proxy_client_->GetConfig(&ctx, req, &rsp);
    if(!status.ok()){
      warn("get config from {} failed", proxy_ip_port_);
    }else{
      if(rsp.pos_list_size() != rsp.ip_port_list_size()){
        critical("get unequally size from config, aborted");
        exit(1);
      }else{
        for(auto i=0;i<rsp.pos_list_size();i++){
          auto pos = rsp.pos_list(i);
          auto host = rsp.ip_port_list(i);
          pos2host_[pos]=host;
        }
        virtual_node_num_ = rsp.virtual_node_num();
        info("get config successfully, node size {}", pos2host_.size());
      }
    }
    std::unique_ptr<PosClientType> pos2kvclient_ptr_tmp(new PosClientType());
    for(auto& pair : pos2host_){
      auto proxy_client = std::make_shared<Bicache::KV_service::Stub>(grpc::CreateChannel(
          pair.second, grpc::InsecureChannelCredentials()));
      (*pos2kvclient_ptr_tmp)[pair.first]= proxy_client; 
    }
    pos2kvclient_ptr_.swap(pos2kvclient_ptr_tmp);
  }

  uint32_t get_key_successor(const std::string& key, uint32_t& key_pos){
    key_pos = MurmurHash64B(key.c_str(), key.length()) % virtual_node_num_;
    auto ite_lower = pos2host_.lower_bound(key_pos);
    uint32_t successor = 0;
    if(ite_lower == pos2host_.end()){
      successor = pos2host_.cbegin()->first;
    }else{
      successor = ite_lower->first;
    }
    return successor;
  }

  std::string Get(const std::string& key) {
    if(pos2host_.empty()){
      warn("pos list is empty, which is impossible for me to find any keys");
      return "";
    }
    uint32_t key_pos=0;
    auto key_successor = get_key_successor(key, key_pos);
    auto ite_client = (*pos2kvclient_ptr_).find(key_successor);
    if(ite_client == (*pos2kvclient_ptr_).end()){
      warn("no client of pos {}", key_successor);
      return "";
    }
    auto kv_client = ite_client->second;
    Bicache::GetRequest req;
    Bicache::GetReply rsp;
    ClientContext ctx;
    req.set_key(key);
    req.set_pos_of_key(key_successor);
    req.set_timestamp(get_miliseconds());
    req.set_req_id(getRandomInt());

    auto status = kv_client->Get(&ctx, req, &rsp);
    if(!status.ok()){
      if(status.error_code()==grpc::StatusCode::UNAVAILABLE){
        GetTology();
      }
      critical("Get req failed, error msg: {}", status.error_message());
      return "";
    }
    if(rsp.is_found()){
      info("get key {}: value {} from pos {}", key, rsp.value(), key_successor);
    }else{
      if(rsp.status_code()==-3){
        warn("this node can't found, jump to {}", rsp.close_pos());
        //待完成
      }else if(rsp.status_code() ==-2){
        warn("repeated req");
      }else if(rsp.status_code() == -1){
        warn("out of time range");
      }
      //后面再补这部分的代码
      return "";
    }
    // only for debug
    return rsp.value();
  }

  int Set(const std::string& key, const std::string& value) {
    if(pos2host_.empty()){
      warn("pos list is empty, which is impossible for me to find any keys");
      return -1;
    }
    uint32_t key_pos=0;
    auto key_successor = get_key_successor(key, key_pos);
    auto ite_client = (*pos2kvclient_ptr_).find(key_successor);
    if(ite_client == (*pos2kvclient_ptr_).end()){
      warn("no client of pos {}", key_successor);
      return -1;
    }

    auto kv_client = ite_client->second;
    Bicache::SetRequest req;
    Bicache::SetReply rsp;
    ClientContext ctx;
    req.set_key(key);
    req.set_pos_of_key(key_successor);
    req.set_timestamp(get_miliseconds());
    req.set_req_id(getRandomInt());
    req.set_update_times(expire_time);
    req.set_value(value);

    auto status = kv_client->Set(&ctx, req, &rsp);
    if(!status.ok()){
      //节点连不上，尝试重建client
      if(status.error_code()==grpc::StatusCode::UNAVAILABLE){
        GetTology();
      }
      critical("Get req failed, error msg: {}", status.error_message());
      return -1;
    }
    if(!rsp.is_set()){
      warn("this node can't set, jump to {}", rsp.close_pos());
      //后面再补这部分的代码
      return -1;
    }
    // only for debug
    info("set key {}: value {} from pos {}", key, rsp.value(), key_successor);
    return 0;
  }
private:
  uint32_t virtual_node_num_;
  std::string proxy_ip_port_;
  std::map<uint32_t, std::string> pos2host_;
  std::shared_ptr<Bicache::ProxyServer::Stub> proxy_client_;
  //这里到不同的节点的client，用指针存储效果会更好，unique_ptr存储的话，就不要读写锁了
  std::unique_ptr<PosClientType> pos2kvclient_ptr_;
};

int main(int argc, char** argv) {

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
  for(auto i = 0 ;i<16;i++){
    std::string key = getRandStr(16);
    std::string value = getRandStr(16);
    keys.push_back(key);
    values.push_back(value);
    kvc.Set(key, value);
  }
  for(auto i = 0 ;i<16;i++){
    std::string key = keys[i];
    kvc.Get(key);
    usleep(400000);
  }
  char input_char='s';
  int i = 15;
  do{
    if(input_char == 's'){
      std::string key = getRandStr(16);
      std::string value = getRandStr(16);
      keys.push_back(key);
      values.push_back(value);
      kvc.Set(key, value);
      i++;
    }else if(input_char == 'g'){
      std::string key = keys[i];
      kvc.Get(key);
    }
    input_char = getchar();
    input_char = getchar();
  }while(input_char!='\n');
  return 0;
}
