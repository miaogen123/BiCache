#include <memory>
#include <limits>
#include <cstdio>
#include <cmath>
#include <atomic>
#include <random>
#include <chrono>
#include <unistd.h>
#include "KV_store.h"

extern std::atomic<int> SystemStatus;
//to record throughput
//TODO::Make it graceful
std::atomic<uint64_t> get_count;
std::atomic<uint64_t> set_count;
uint64_t get_throughput=0;
uint64_t set_throughput=0;
std::atomic<uint64_t> prepare_count;
std::atomic<uint64_t> commit_count;
uint64_t prepare_throughput=0;
uint64_t commit_throughput=0;

void output_throughput(int signum){
  info("get_throughput {}, set_throughput {}, prepare_throughput {}, commit_throughput {}", get_throughput, set_throughput, prepare_throughput, commit_throughput);
}

KV_store_impl::KV_store_impl(Conf& conf):inner_conf_(conf){
  auto bucket_size = conf.get("bucket_size", "10000");
  inner_cache_.reserve(atoi(bucket_size.c_str()));
  //每秒
  auto read_limitation = conf.get("read_limitation", "0");
  read_limit_ = atoi(read_limitation.c_str());
  info("enable read limitation, size {}", read_limit_);
  //inner_cache_["hello"]=std::make_pair<uint64_t, std::string>(get_miliseconds(), "world");
  increment_data_keys_.reset(new std::vector<std::string>());
  backup_increment_data_keys_.reset(new std::vector<std::string>());
  signal(SIGINT, output_throughput);
}

void KV_store_impl::init(){
  CH_node_serving_thr_ = std::make_shared<std::thread>(&KV_store_impl::run_CH_node, this);
  backend_update_thr_ = std::make_shared<std::thread>(&KV_store_impl::backend_update, this);
}

const cache_type& KV_store_impl::split_inner_cache(){
  return inner_cache_;
}

cache_type& KV_store_impl::get_mutable_inner_cache(){
  return inner_cache_;
}

std::shared_mutex& KV_store_impl::get_inner_cache_lock(){
  return rw_lock_for_cache_;
}

BackupData& KV_store_impl::get_backup(){
  return backup_;
}
std::unique_ptr<std::vector<std::string>>& KV_store_impl::get_increment_data(){
  debug("KV:increment data size {}", increment_data_keys_->size());
  return increment_data_keys_;
}

std::unique_ptr<std::vector<std::string>>& KV_store_impl::get_backup_increment_data(){
  debug("KV:backup increment data size {}", backup_increment_data_keys_->size());
  return backup_increment_data_keys_;
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

  //read replica 没有加锁可能会有问题，这里留个心
  //不做过多的验证，有数据就行
  get_count++;
  if(req->read_replica()){
    std::shared_lock<std::shared_mutex> r_lock(backup_.lock);
    auto backup_cache = backup_.backup_cache;
    auto ite = backup_cache.find(key);
    //判断是否是事务-不判断了
    //std::shared_lock<std::shared_mutex> w_lock_for_trans(rw_lock_for_trans_);
    //if(keys_in_trans_.find(key)!=keys_in_trans_.end()){
    //  debug("key {} op aborted because the key is in transactions");
    //  return {grpc::StatusCode::ABORTED, "key in in transaction"};
    //}
    //w_lock_for_trans.unlock();

    if(ite==backup_cache.end()){
      rsp->set_value("");
    }else if(ite->second.first < get_miliseconds()){
      //超时就取不到
      rsp->set_status_code(-4);
      //debug("KV: read replica: get key {} failed because of expire", key);
    }else{
      rsp->set_is_found(true);
      rsp->set_value(ite->second.second);
    }
    //info("KV: read replica req key {}, founded: {}", key, rsp->is_found()?"true":"false");
    return {grpc::StatusCode::OK, ""};
  }
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
    //开启了read_limit_
    if(read_limit_!=0){
      posStatus_[key_pos].cur_count++;
      rsp->set_node_overloaded(posStatus_[key_pos].overloaded);
    }
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
  if(SystemStatus.load()!=0){
    info("KV:system status is {}", SystemStatus.load());
    return {grpc::StatusCode::ABORTED, "system is maintaning"};
  }
  set_count++;
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
    return {grpc::StatusCode::ABORTED, ""};
  }

  do{
    //判断是否是事务
    std::shared_lock<std::shared_mutex> w_lock_for_trans(rw_lock_for_trans_);
    if(keys_in_trans_.find(key)!=keys_in_trans_.end()){
      debug("key {} op aborted because the key is in transactions");
      return {grpc::StatusCode::ABORTED, "key in in transaction"};
    }
    w_lock_for_trans.unlock();

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
      // update_time 为0 就设置成最大的
      ite->second.first = std::numeric_limits<uint64_t>::max();
    }
    increment_data_keys_->push_back(key);
  }while(false);
  //流量小的时候可以，大的时候就不能打这种log了
  //info("KV:Get from ")
  return {grpc::StatusCode::OK, ""};
}

//一个是删除占用的资源，一个是删除这个事务
void KV_store_impl::release_resource_in_transaction(int req_id){
  auto ite = transactions_.transactions.find(req_id);
  if(ite==transactions_.transactions.end()){
    debug("req_id {} released already", req_id);
    return;
  }

  auto& single_trans = ite->second;
  std::unique_lock<std::shared_mutex> w_lock(rw_lock_for_trans_);
  std::string tmp_key;
  for(auto& key:single_trans.keys){
    //TODO::only for debug
    tmp_key+=(key+" ");
    keys_in_trans_.erase(key);
  }
  debug("KV: releasing transaction resource, erase key {}",tmp_key);
  w_lock.unlock();

  //delete transaction
  std::unique_lock<std::shared_mutex> w_lock_to_erase(transactions_.w_lock);
  transactions_.transactions.erase(req_id);
}

//prepare 锁定资源
::grpc::Status KV_store_impl::TransactionPrepare(::grpc::ServerContext* context, const ::Bicache::TransactionStepRequest* req, ::Bicache::TransactionStepReply* rsp){
  prepare_count++;
  debug("KV:transaction:prepare req_id:{} ", req->req_id());
  //构建 transaction
  SingleTransaction single_trans;
  single_trans.expiera_at=get_miliseconds()+req->req_time_out();
  for(auto i=0;i<req->keys_size();i++){
    auto pos = MurmurHash64B(req->keys(i).c_str(), req->keys(i).size())%virtual_node_num_;
    if(ch_node_->in_range(pos)){
      single_trans.keys.push_back(req->keys(i));
      single_trans.operation_id.push_back(req->operation_id(i));
      if(req->operation_id(i)==0)
        single_trans.values.push_back(req->values(i));
    }else{
      debug("KV:trans aborted becauseof key {} not in my range");
      release_resource_in_transaction(req->req_id());
      return {grpc::StatusCode::OK, ""};
    }
  }

  //占据资源
  std::unique_lock<std::shared_mutex> w_lock(rw_lock_for_trans_);
  for(auto& key:single_trans.keys){
    keys_in_trans_.insert(key);
    debug("KV:lock key in map {}", key);
  }
  w_lock.unlock();

  //事务入库
  std::unique_lock<std::shared_mutex> w_lock_to_insert_trans(transactions_.w_lock);
  transactions_.transactions.insert({req->req_id(), std::move(single_trans)}); 
  return {grpc::StatusCode::OK, ""};
}

::grpc::Status KV_store_impl::TransactionCommit(::grpc::ServerContext* context, const ::Bicache::TransactionStepRequest* req, ::Bicache::TransactionStepReply* rsp){
  //不再检测正确性，直接搞set
  commit_count++;
  debug("KV:transaction:commit req_id:{} ", req->req_id());
  std::unique_lock<std::shared_mutex> w_lock_to_check(transactions_.w_lock);
  auto ite= transactions_.transactions.find(req->req_id());
  if(ite==transactions_.transactions.end()){
    debug("req_id {} already expired", req->req_id());
    return {grpc::StatusCode::ABORTED, ""};
  }
  w_lock_to_check.unlock();
  int count=0;
  //TODO::should write with rw_lock
  std::string value_to_ret("");
  for(auto i=0;i<ite->second.keys.size();i++){
    auto key = ite->second.keys[i];
    auto operation_id = ite->second.operation_id[i];
    auto ite_to_ret=inner_cache_.find(key);
    if(0==operation_id){
      auto value = ite->second.values[i];
      //debug("commit write key {} {}", key, value);
      if(ite_to_ret!=inner_cache_.end()){
        ite_to_ret->second.first=std::numeric_limits<uint64_t>::max();
        ite_to_ret->second.second=value;
      }else{
        inner_cache_.insert({key, std::make_pair<uint64_t, std::string>(std::numeric_limits<uint64_t>::max(), std::move(value))});
      }
    }else{
      if(ite_to_ret!=inner_cache_.end()){
        value_to_ret=ite_to_ret->second.second;
      }
      //WARNING这个地方需要注意，可能会因为顺序出现问题
      rsp->add_keys(key);
      rsp->add_values(value_to_ret);
      //debug("commit read key {} {}", key, value_to_ret);
    }
    count++;
  }
  debug("req_id {} update {} keys", ite->first, count);
  release_resource_in_transaction(ite->first);
  return {grpc::StatusCode::OK, ""};
}

::grpc::Status KV_store_impl::TransactionRollback(::grpc::ServerContext* context, const ::Bicache::TransactionStepRequest* req, ::Bicache::TransactionStepReply* rsp){
  debug("KV:transaction:rollback req_id:{} ", req->req_id());
  release_resource_in_transaction(req->req_id());
  return {grpc::StatusCode::OK, ""};
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
  ch_node_->set_kv_store_p(this);
  //CH_node_impl node{inner_conf};
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(ch_node_.get());
  builder.SetMaxSendMessageSize(INT_MAX);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("KV:CH node listening on {}", server_address);
  ch_node_->run();
  virtual_node_num_ = ch_node_->get_virtual_node_num_();
  posStatus_.resize(virtual_node_num_);
  server->Wait();
}

void KV_store_impl::backend_update(){
  info("KV:start cleaning: cleaning interval = {}, info log interval = 30*interval", clean_interval_);
  //TODO::这里先清理reqids，后面添加了 key 超时以后再设置 key 清理的逻辑
  int count = 0;
  int expire_clean_count = 0;
  int UP_COUNT=30;
  do{
    count++;
    usleep(clean_interval_);
    //清理过期的req，防重放
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
    //清理过期的keys
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

    //clean the keys not in my range() and  not in prenode 
    //note: case(keys not in my range usually)happens when node joins, because node joins not so frequently, 
    //this operation run a few
    //清理已经分出去的keys(上个节点负责的先不清)
    if(count == 20 && ch_node_->get_pp_node()!=-1){
      std::vector<cache_type::iterator> ite_to_be_deleted;
      std::shared_lock<std::shared_mutex> r_lock(rw_lock_for_cache_);
      for(auto ite = inner_cache_.begin();ite!=inner_cache_.end();ite++) {
        //TODO:: this can be optimized 
        auto key_pos = MurmurHash64B(ite->first.c_str(), ite->first.size())%virtual_node_num_;
        if(!ch_node_->in_range(key_pos, ch_node_->get_pp_node())){
          ite_to_be_deleted.push_back(ite);
        }else{
          //test with a few of total keys
          //debug(" key {} pos {} not in range {}", ite->first, key_pos, ch_node_->get_pp_node());
        }
      }
      r_lock.unlock();
      std::shared_lock<std::shared_mutex> w_lock(rw_lock_for_cache_);
      for(auto& ite:ite_to_be_deleted){
        inner_cache_.erase(ite);
      }
      info("KV:clean not-in-range keys periodically: {}", ite_to_be_deleted.size());
    }

//TEST
//  while (!expire_queue_.empty()) {
//      const timed_key& tk = expire_queue_.top();
//      auto it = tk.second;
//      info("KV:clean {} value {} timestamp {} cur {} queue.size{} inner_cache_.size {}", it->first, it->second.second, it->second.first, curr_ts, expire_queue_.size(), inner_cache_.size());
//      inner_cache_.erase(it);
//      expire_queue_.pop();
//  }
      //这里让他每秒走一次就好了，因为量不大，就直接遍历就好了
      //用来计算吞吐
      if(count==10){
        int time_elasp = UP_COUNT * clean_interval_ /1000000 ;
        uint64_t cur_count =0;
        uint64_t pre_count =0;
        for(int i=0;i<virtual_node_num_;i++){
          cur_count = posStatus_[i].cur_count;
          pre_count = posStatus_[i].pre_count;
          if(cur_count!=0){
            auto throughput = ((cur_count - pre_count)/time_elasp );
            if(throughput > read_limit_){
              info("KV:pos{}, get read_throughput {} in {}s, overloaded", i, throughput, time_elasp);
              posStatus_[i].overloaded= true;
            }else{
              posStatus_[i].overloaded= false;
            }
            posStatus_[i].pre_count=cur_count;
          }
        }
        //TODO::Make it graceful
        static uint64_t pre_set=0, pre_get=0, pre_prepare=0, pre_commit=0;
        auto cur_get=get_count.load();
        auto cur_set=set_count.load();
        auto cur_prepare=prepare_count.load();
        auto cur_commit=commit_count.load();
        get_throughput=(cur_get-pre_get)/time_elasp;
        set_throughput=(cur_set-pre_set)/time_elasp;
        prepare_throughput=(cur_prepare-pre_prepare)/time_elasp;
        commit_throughput=(cur_commit-pre_commit)/time_elasp;
        pre_get=cur_get;
        pre_set=cur_set;
        pre_prepare=cur_prepare;
        pre_commit=cur_commit;
        //debug("get {} set {} time{}", get_throughput, set_throughput, time_elasp);
      }
      //interval=200ms时，6s清理一次
      if(count==10){
        std::unique_lock<std::shared_mutex> w_lock_to_check(transactions_.w_lock);
        auto cur_ts = get_miliseconds();
        std::vector<int> ids;
        for(auto ite=transactions_.transactions.begin(); ite != transactions_.transactions.end();ite++){
          if(ite->second.expiera_at <= cur_ts){
            ids.push_back(ite->first);
          }
        }
        w_lock_to_check.unlock();
        for(auto& id:ids){
          release_resource_in_transaction(id);
          debug("KV:periodically release resourse of id {}", id);
        }
      }

      if(count==UP_COUNT){
        info("KV:clean log: clean {} expire req_ids_, keysize {}", expire_clean_count++, inner_cache_.size());
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
