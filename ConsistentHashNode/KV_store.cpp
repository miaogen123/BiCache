#include <memory>
#include <cstdio>
#include <cmath>
#include <random>
#include <chrono>
#include <unistd.h>
#include "KV_store.h"

//TODO::这个函数在ProxyImpl.cpp里面也有，代码略丑，待优化
uint64_t get_miliseconds(){
    using namespace std::chrono;
    steady_clock::duration d;
    d = steady_clock::now().time_since_epoch();
    return duration_cast<milliseconds>(d).count();
}

KV_store_impl::KV_store_impl(Conf& conf):inner_conf_(conf){
    auto bucket_size = conf.get("bucket_size", "10000");
    inner_cache_.reserve(atoi(bucket_size.c_str()));
}

//TODO:: 有些接口定义的不是很同一（同时存在 Response&Reply）
//里面做的事情：判断是否过期，取值 or 查找最近
::grpc::Status KV_store_impl::Get(::grpc::ServerContext* context, const ::Bicache::GetRequest* req, ::Bicache::GetReply* rsp){
  auto key_pos = req->pos_of_key();
  auto& key = req->key();
  auto timestamp = req->timestamp();
  auto cur_timestamp = get_miliseconds();
  //是否发出超过3s的“幽灵”包？
  if(cur_timestamp>timestamp + 2000){
    rsp->set_is_found(false);
    return {grpc::StatusCode::ABORTED, "your req is out-of-dated"};
  }
  //是否是3秒内的重复数据包?
  //为了安全这里还是需要加上锁的
  {
    std::shared_lock<std::shared_mutex> r_lock(rw_lock_for_ids_);
    auto ite = req_ids_.find(req->req_id());
    if(ite!=req_ids_.end()){
      r_lock.unlock();
      std::lock_guard<std::shared_mutex> w_lock(rw_lock_for_ids_);
      ite->second = get_miliseconds()+2000;
    }
  }
  rsp->set_is_found(false);
  if(ch_node_->in_range(key_pos)){
    auto ite = inner_cache_.find(key);
    if(ite==inner_cache_.end()){
    }else{
      rsp->set_is_found(true);
      rsp->set_value(ite->second);
    }
  }else{
    int close_one = -1;
    ch_node_->find_closest_preceding_finger(key_pos, close_one, ch_node_->get_finger_table());
    rsp->set_close_pos(close_one);
  }
  //流量小的时候可以，大的时候就不能打这种log了
  //info("Get from ")
}

void KV_store_impl::init(){
  CH_node_serving_thr_ = std::make_shared<std::thread>(&KV_store_impl::run_CH_node, this);
  backend_update_thr_ = std::make_shared<std::thread>(&KV_store_impl::backend_update, this);
}

//单独的线程用来服务
void KV_store_impl::run_CH_node(){
  std::string CH_node_port = inner_conf_.get("CH_node_port");
  if(CH_node_port.size()==0){
    spdlog::critical("get CH_node_port error");
    exit(-1);
  }
  
  std::string server_address("0.0.0.0:"+CH_node_port);
  ch_node_ = std::make_shared<CH_node_impl>(inner_conf_);
  //CH_node_impl node{inner_conf};
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(ch_node_.get());
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("CH node listening on {}", server_address);
  ch_node_->run();
  server->Wait();
}

void KV_store_impl::backend_update(){
  info("start cleaning: cleaning interval = 2s, info log interval = 10s");
  //TODO::这里先清理reqids，后面添加了 key 超时以后再设置 key 清理的逻辑
  int count = 0;
  int expire_clean_count = 0;
  do{
    count++;
    usleep(clean_interval_);
    {
      std::unique_lock<std::shared_mutex> w_lock_for_ids(rw_lock_for_ids_);
      auto cur_miliseconds = get_miliseconds();
      //2s内的请求应该不多，直接遍历就好
      for(auto ite = req_ids_.begin();ite!= req_ids_.end();){
        if(ite->second < cur_miliseconds){
          ite = req_ids_.erase(ite);
          expire_clean_count++;
        }else{
          ite++;
        }
      }
    }
    if(count==5){
      info("clean log: clean {} expire req_ids_", expire_clean_count++);
      count=0;
      expire_clean_count=0;
    }
  }while(!exit_clean_flag_);
}

void KV_store_impl::run(){
  //设置一个静态局部变量，返回的只有一个
}

KV_store_impl::~KV_store_impl(){
  CH_node_serving_thr_->join();
  backend_update_thr_->join();
}