#include <memory>
#include <limits>
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
    inner_cache_["hello"]=std::make_pair<uint64_t, std::string>(get_miliseconds(), "world");
}

int KV_store_impl::is_valid(uint64_t& timestamp, int& req_id, uint64_t& key_pos, std::string& msg){
  auto cur_timestamp = get_miliseconds();
  //是否发出超过2s的“幽灵”包？
  //是否是2秒内的重复数据包?
  if(cur_timestamp>timestamp + 2000){
    msg = "your req is out-of-dated";
    return -1;
  }
  //为了安全这里还是需要加上锁的
  {
    std::shared_lock<std::shared_mutex> r_lock(rw_lock_for_ids_);
    auto ite = req_ids_.find(req_id);
    if(ite==req_ids_.end()){
      r_lock.unlock();
      std::lock_guard<std::shared_mutex> w_lock(rw_lock_for_ids_);
      req_ids_[req_id]= get_miliseconds()+2000;
    }else{
      msg = "get the same req in the past two seconds";
      return -2;
    }
  }
  if(!ch_node_->in_range(key_pos)){
    msg = "this key is not in my range";
    return -3;
  }
  return 0;
}

//TODO:: 有些接口定义的不是很同一（同时存在 Response&Reply）
//里面做的事情：判断是否过期，取值 or 查找最近
::grpc::Status KV_store_impl::Get(::grpc::ServerContext* context, const ::Bicache::GetRequest* req, ::Bicache::GetReply* rsp){
  uint64_t key_pos = req->pos_of_key();
  auto& key = req->key();
  auto timestamp = req->timestamp();
  std::string msg("");
  int req_id = req->req_id();
  rsp->set_is_found(false);

  auto ret= is_valid(timestamp, req_id, key_pos, msg);
  // ret == -3:key is not in range
  if(ret == -3){
    int close_one = -1;
    ch_node_->find_closest_preceding_finger(key_pos, close_one, ch_node_->get_finger_table());
    info("KV:close one {} to key_pos {}", close_one, key_pos);
    rsp->set_close_pos(close_one);
  }
  if(ret<0){
    rsp->set_status_code(ret);
    return {grpc::StatusCode::OK, ""};
  }
  {
    std::shared_lock<std::shared_mutex> r_lock_for_cache(rw_lock_for_cache_);
    auto ite = inner_cache_.find(key);
    if(ite==inner_cache_.end()){
      rsp->set_value("");
    }else if(ite->second.first < get_miliseconds()){
      //超时就取不到
      rsp->set_status_code(-4);
      debug("KV:get key {} failed because of expire", key);
    }else{
      rsp->set_is_found(true);
      rsp->set_value(ite->second.second);
    }
  }
  //流量小的时候可以，大的时候就不能打这种log了
  //info("KV:Get from ")
  return {grpc::StatusCode::OK, ""};
}

::grpc::Status KV_store_impl::Set(::grpc::ServerContext* context, const ::Bicache::SetRequest* req, ::Bicache::SetReply* rsp){
  uint64_t key_pos = req->pos_of_key();
  auto& key = req->key();
  auto timestamp = req->timestamp();
  std::string msg("");
  int req_id = req->req_id();
  rsp->set_is_set(false);

  auto ret= is_valid(timestamp, req_id, key_pos, msg);
  // ret == -3:key is not in range
  if(ret == -3){
    int close_one = -1;
    ch_node_->find_closest_preceding_finger(key_pos, close_one, ch_node_->get_finger_table());
    rsp->set_close_pos(close_one);
  }
  if(ret<0){
    rsp->set_status_code(ret);
    return {grpc::StatusCode::OK, ""};
  }
  {
    std::unique_lock<std::shared_mutex> w_lock_(rw_lock_for_cache_);
    auto ite = inner_cache_.find(key);
    rsp->set_is_set(true);
    std::string value = req->value();
    uint64_t timestamp = get_miliseconds();
    if(ite==inner_cache_.end()){
      ite = inner_cache_.insert({key, std::make_pair<uint64_t, std::string>(std::move(timestamp), std::move(value))}).first;
    }else{
      ite->second.second = value;
    }
    //info("KV:set key {} value {} {}", key, req->value(), ite->second.first);
    if(req->update_times()>0){
      ite->second.first = (timestamp + req->update_times());
      expire_queue_.push( std::make_pair(ite->second.first, ite) );
      info("KV:update expire time {} to {}: update_times {}",  key, timestamp, req->update_times());
    }else{
      ite->second.first = std::numeric_limits<uint64_t>::max();
    }
  }
  //流量小的时候可以，大的时候就不能打这种log了
  //info("KV:Get from ")
  return {grpc::StatusCode::OK, ""};
}

void KV_store_impl::init(){
  CH_node_serving_thr_ = std::make_shared<std::thread>(&KV_store_impl::run_CH_node, this);
  backend_update_thr_ = std::make_shared<std::thread>(&KV_store_impl::backend_update, this);
}

//单独的线程用来服务
void KV_store_impl::run_CH_node(){
  std::string CH_node_port = inner_conf_.get("CH_node_port");
  if(CH_node_port.size()==0){
    spdlog::critical("KV:get CH_node_port error");
    exit(-1);
  }

  std::string server_address("0.0.0.0:"+CH_node_port);
  ch_node_ = std::make_shared<CH_node_impl>(inner_conf_);
  //CH_node_impl node{inner_conf};
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(ch_node_.get());
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("KV:CH node listening on {}", server_address);
  ch_node_->run();
  server->Wait();
}

void KV_store_impl::backend_update(){
  info("KV:start cleaning: cleaning interval = 2s, info log interval = 10s");
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
        //TODO::这里看后面吧，要是影响比较大，就不写了。不多个锤子很多的
        for(auto ite = req_ids_.begin();ite!= req_ids_.end();){
          if(ite->second < cur_miliseconds){
            ite = req_ids_.erase(ite);
            expire_clean_count++;
          }else{
            ite++;
          }
        }
      }
      uint64_t curr_ts = get_miliseconds();
      {
        std::unique_lock<std::shared_mutex> lock(rw_lock_for_cache_);
        while (!expire_queue_.empty()) {
            const timed_key& tk = expire_queue_.top();
            //debug("KV:tk {} second {} {} {} {}", tk.first, tk.second->first, tk.second->second.first, tk.second->second.second, curr_ts);
            if (tk.first < curr_ts) {
                auto it = tk.second;
                if (it != inner_cache_.end() && it->second.first < curr_ts) {
                    info("KV:clean {} value {} timestamp {} cur {} queue.size{}", it->first, it->second.second, it->second.first, curr_ts, expire_queue_.size());
                    inner_cache_.erase(it);
                }
                expire_queue_.pop();
            } else {
                break;
            }
        }
      }
      //TEST
//      while (!expire_queue_.empty()) {
//          const timed_key& tk = expire_queue_.top();
//          auto it = tk.second;
//          info("KV:clean {} value {} timestamp {} cur {} queue.size{} inner_cache_.size {}", it->first, it->second.second, it->second.first, curr_ts, expire_queue_.size(), inner_cache_.size());
//          inner_cache_.erase(it);
//          expire_queue_.pop();
//      }


      if(count==60){
        info("KV:clean log: clean {} expire req_ids_", expire_clean_count++);
        count=0;
        expire_clean_count=0;
      }
    }while(!exit_clean_flag_);
}

void KV_store_impl::run(){
  //设置一个静态局部变量，返回的只有一个
}

KV_store_impl::~KV_store_impl(){
  exit_clean_flag_ = false;
  CH_node_serving_thr_->join();
  backend_update_thr_->join();
}