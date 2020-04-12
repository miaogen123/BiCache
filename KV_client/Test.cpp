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

struct NodeStatus{
  bool overloaded = false;
  std::string inner_host;
  NodeStatus(){}
  NodeStatus(const std::string& host){
    inner_host = host;
  }
  NodeStatus(const NodeStatus& nodeStatus){
    inner_host = nodeStatus.inner_host;
    overloaded = nodeStatus.overloaded;
  }
};

class KV_client{
public: 
  using PosClientType = std::unordered_map<uint32_t, std::shared_ptr<Bicache::KV_service::Stub>>;
  KV_client(Conf conf){
    auto proxy_ip = conf.get("proxy_ip");
    auto proxy_port = conf.get("proxy_port");
    auto read_backup_flag = conf.get("read_backup", "");
    if(read_backup_flag.size()!=0){
      read_backup_  = true;
    }
    proxy_ip_port_ = proxy_ip+":"+proxy_port;
    proxy_client_ = std::make_shared<Bicache::ProxyServer::Stub>(grpc::CreateChannel(
        proxy_ip_port_, grpc::InsecureChannelCredentials() ));
  }

  void GetTology(){
    ClientContext ctx;
    Bicache::GetConfigRequest req;
    Bicache::GetConfigReply rsp;
    auto status = proxy_client_->GetConfig(&ctx, req, &rsp);
    std::unique_ptr<PosClientType> pos2kvclient_ptr_tmp(new PosClientType());
    if(!status.ok()){
      warn("get config from {} failed", proxy_ip_port_);
    }else{
      if(rsp.pos_list_size() != rsp.ip_port_list_size()){
        critical("get unequally size from config, aborted");
        exit(1);
      }else{
        std::unique_lock<std::shared_mutex> w_lock(rw_lock_for_pos2host_);
        pos2host_.clear();
        for(auto i=0;i<rsp.pos_list_size();i++){
          auto pos = rsp.pos_list(i);
          auto host = rsp.ip_port_list(i);
          pos2host_[pos] = NodeStatus(host);
          info("pos {}, host {}", pos, host);
          auto proxy_client = std::make_shared<Bicache::KV_service::Stub>(grpc::CreateChannel(
              host, grpc::InsecureChannelCredentials()));
          (*pos2kvclient_ptr_tmp)[pos]= proxy_client; 
        }
        virtual_node_num_ = rsp.virtual_node_num();
        info("get config successfully, node size {}", pos2host_.size());
      }
    }
    //用指针可以避免加锁
    pos2kvclient_ptr_.swap(pos2kvclient_ptr_tmp);
  }

  uint32_t get_key_successor(const std::string& key, uint32_t& key_pos, bool& overloaded, bool find_backup=false){
    auto hash_value = MurmurHash64B(key.c_str(), key.length());
    key_pos = hash_value % virtual_node_num_;
    std::shared_lock<std::shared_mutex> w_lock(rw_lock_for_pos2host_);
    auto ite_lower = pos2host_.lower_bound(key_pos);
    uint32_t successor = 0;
    if(ite_lower == pos2host_.end()){
      successor = pos2host_.cbegin()->first;
    }else{
      successor = ite_lower->first;
    }

    if(find_backup || pos2host_[successor].overloaded ){
      //debug("test {}  {}", find_backup, pos2host_[successor].overloaded);
      static std::default_random_engine generator;
      static std::uniform_int_distribution<int> dis(0,1); 
      if(dis(generator)==0){
        overloaded = false;
        return successor;
      }
      auto origin_succ = successor;
      auto ite_backup = pos2host_.lower_bound(successor+1);
      if(ite_backup == pos2host_.end()){
        successor = pos2host_.cbegin()->first;
      }else{
        successor = ite_backup->first;
      }
      //info("find backup {} origin is {}", successor, origin_succ);
      overloaded = true;
    }
    //debug("success returned {}", successor);
    return successor;
  }

  std::string Get(const std::string& key) {
    if(pos2host_.empty()){
      warn("pos list is empty, which is impossible for me to find any keys");
      return "";
    }
    uint32_t key_pos=0;
    bool node_overloaded = false;
    auto key_successor = get_key_successor(key, key_pos, node_overloaded);
    auto ite_client = (*pos2kvclient_ptr_).find(key_successor);
    if(ite_client == (*pos2kvclient_ptr_).end()){
      warn("no client of pos {}", key_successor);
      return "";
    }else{
      info("get client of pos {}, key {}", key_successor, key);
    }
    auto kv_client = ite_client->second;
    Bicache::GetRequest req;
    Bicache::GetReply rsp;
    ClientContext ctx;
    req.set_key(key);
    req.set_read_replica(node_overloaded);
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
      info("404 not found", key, rsp.value(), key_successor);
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
    //primary overloaded
    if(rsp.node_overloaded()&&read_backup_){
      auto ite = pos2host_.find(key_successor);
      if(ite == pos2host_.end()){
        critical("find host {} in pos2host failed, bad logic", key_successor);
      }else{
        pos2host_[key_successor].overloaded=true;
      }
      debug("found node {} overloaded", key_successor);
    }
    // only for debug
    return rsp.value();
  }

  int Set(const std::string& key, const std::string& value) {
    //pos2host_ 理论上是应该用shared_mutex来保护的
    if(pos2host_.empty()){
      warn("pos list is empty, which is impossible for me to find any keys");
      return -1;
    }
    uint32_t key_pos=0;
    //TODO::这里其实多算了一次
    bool node_overloaded = false;
    auto key_successor = get_key_successor(key, key_pos, node_overloaded);
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
      critical("Set req failed, error msg: {}", status.error_message());
      return -1;
    }
    if(!rsp.is_set()){
      warn("this node can't set, jump to {}", rsp.close_pos());
      //后面再补这部分的代码
      return -1;
    }
    // only for debug
    //auto key_hash = MurmurHash64B(key.c_str(), key.size())%virtual_node_num_;
    info("set key {}: value {} keypos {} from pos {}", key, value, key_pos, key_successor);
    return 0;
  }
  int Transaction(const std::vector<std::string>& keys, const std::vector<std::string>& values){
    //提交事务
    if(keys.size()!=values.size()){
      critical("transaction input with diff length:{} {}",keys.size(), values.size());
      return -1;
    }
    Bicache::TransactionRequest req;
    Bicache::TransactionReply rsp;
    req.set_req_id(getRandomInt());
    for(auto i =0 ;i<keys.size();i++){
      req.add_keys(keys[i]);
      req.add_values(values[i]);
    }
    grpc::ClientContext ctx;
    auto status = proxy_client_->Transaction(&ctx, req, &rsp);
    if(!status.ok()){
      warn("reqid {} transaction failed with msg:{}", req.req_id(), status.error_message());
      return -1;
    }
    return 0;
  }
private:
  uint32_t virtual_node_num_;
  bool read_backup_ = false;
  std::string proxy_ip_port_;
  std::shared_mutex rw_lock_for_pos2host_;
  std::map<uint32_t, NodeStatus> pos2host_;
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