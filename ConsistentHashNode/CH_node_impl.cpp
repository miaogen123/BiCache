#include <memory>
#include <cstdio>
#include <cmath>
#include <random>
#include <unistd.h>
#include "CH_node_impl.h"
#include "KV_store.h"
#include "../MyUtils/cpp/hash.h"


//TODO::这个函数在ProxyImpl.cpp里面也有，代码略丑，待优化
uint64_t get_miliseconds(){
    using namespace std::chrono;
    system_clock::duration d;
    d = system_clock::now().time_since_epoch();
    return duration_cast<milliseconds>(d).count();
}

//to communicate with proxyServer 

CH_node_impl::CH_node_impl(Conf& conf){
  //init proxyClient
  auto proxy_ip = conf.get("proxy_ip");
  if(proxy_ip.size() == 0){
      critical("whthout proxy ip, service starts failed");
      exit(-1);
  }
  kv_port_ = conf.get("KV_port");
  if(kv_port_.empty()){
    critical("without KV port, aborted");
    exit(-1);
  }
  auto proxy_port = conf.get("proxy_port", "7790");
  // get client
  proxy_client_ = std::move(std::make_unique<Bicache::ProxyServer::Stub>(grpc::CreateChannel(
      proxy_ip + ":" + proxy_port, grpc::InsecureChannelCredentials() )));
  //TODO:: get local function
  cur_host_ip_ = conf.get("host_ip");
  cur_host_port_ = conf.get("CH_node_port");
  auto mbit_str = conf.get("mbit", "4");
  auto to_ret = std::atoi(mbit_str.c_str());
  if(mbit_str.size()==0||to_ret==0){
    mbit = 4;
  }else{
    mbit = to_ret;
  }
  auto stablize_interval=conf.get("stablize_interval", "100000");
  stablize_interval_ = std::atoi(stablize_interval.c_str());
  update_thr_ = std::make_shared<std::thread>(&CH_node_impl::HB_to_proxy, this);
  stablize_thr_ = std::make_shared<std::thread>(&CH_node_impl::stablize, this);
  push_data_thr_ = std::make_shared<std::thread>(&CH_node_impl::push_increment_data, this);
}

void CH_node_impl::notify_to_proxy(){
  RegisterRequest req;
  RegisterReply reply;
  req.set_ip(cur_host_ip_);
  req.set_port(cur_host_port_);
  req.set_kv_port(kv_port_);
  req.set_pos(cur_pos_);
  ClientContext context;
  auto status = proxy_client_->Register(&context, req, &reply);
  if(status.ok()){
    info("notify proxy SUCCESS, ready to service");
  }else{
    critical("notify proxy FAILED");
    //TODO:: you should retry 
    exit(-1);
  }
}

int CH_node_impl::register_to_proxy(const std::string& host, const std::string& port){
  RegisterRequest req;
  RegisterReply reply;
  req.set_ip(host);
  req.set_port(port);
  req.set_kv_port(kv_port_);
  req.set_pos(-1);
  ClientContext context;
  auto status = proxy_client_->Register(&context, req, &reply);
  if(status.ok()){
    cur_pos_ = reply.pos();
    next_node_ip_port_ = reply.next_node_ip_port();
    virtual_node_num_ = reply.total_range();
    if(next_node_ip_port_ == host+":"+port){
      //initialize first finger table
      auto interval_start = (cur_pos_ + 1)%virtual_node_num_;
      for(int i=1;i<=mbit;i++){
        Bicache::FingerItem item;
        item.set_start(interval_start);
        item.set_interval(pow(2.0, double(i-1)));
        item.set_successor(cur_pos_);
        item.set_ip_port(cur_host_ip_+":"+cur_host_port_);
        interval_start = (interval_start + item.interval())%virtual_node_num_;
        finger_table_.push_back(std::move(item));
      }
      pre_node_ = cur_pos_;
      pos2host_[cur_pos_]=cur_host_ip_+":"+cur_host_port_;
      pre_node_ip_port_ = cur_host_ip_ +":"+cur_host_port_;
      //first node notify to proxy
      notify_to_proxy();
    }else{
      pos2host_[cur_pos_]=cur_host_ip_+":"+cur_host_port_;
      pos2host_[reply.next_node_pos()]= reply.next_node_ip_port();
    }
    info("{} next node is {}", cur_pos_, reply.next_node_pos());
    std::string msg = "register OK, cur_pos_: "+ std::to_string(reply.pos()) + " total_range:" \
      + std::to_string(reply.total_range()) +" next_ip:"+ reply.next_node_ip_port();
    next_pos_ = reply.next_node_pos();
    info(msg);
    return 0;
  }else{
    critical(status.error_message().c_str());
    return -1;
  }
}


// addnode 的操作必须要是序列化的
Status CH_node_impl::AddNode(::grpc::ServerContext* context, const ::Bicache::AddNodeRequest* req, ::Bicache::AddNodeReply* reply){
  auto ip = req->ip();
  auto port = req->port();
  auto pos = req->pos();
  {
    auto range_start = ( pre_node_ + 1 )%virtual_node_num_;
    if(range_start < cur_pos_ &&(pos<range_start || pos >cur_pos_ )){
      reply->set_status(-1);
      debug("Ser: get addreq from pos {}, but not in may range {}~{}", pos, range_start, cur_pos_);
      return {grpc::StatusCode::ABORTED, "your pos is not in my responsibility, find another one"};
    } else if(range_start >= cur_pos_&& (pos< cur_pos_ && pos > range_start)){
      reply->set_status(-1);
      debug("Ser: get addreq from pos {}, but not in may range {}~{}", pos, range_start, cur_pos_);
      return {grpc::StatusCode::ABORTED, "your pos is not in my responsibility, find another one"};
    }
    if(cur_pos_ - range_start ==1){
      reply->set_status(-1);
      return {grpc::StatusCode::ABORTED, "my range has been limited to 1"};
    }
    //TODO:: 这里切分需要注意是原子性的，不能自己范围切了然后对方没有接受到数据。所以需要等对方把数据给切分了以后发送过来一个ack包，自己才停止对外服务
    //这个地方是有一个中间状态的，在这个中间状态需要不对外提供写服务，只提供读服务。
    //TODO:: 似乎这里的赋值都只能使用遍历？？？脑阔痛
    for(int i=0;i<finger_table_.size();i++){
      reply->add_finger_table()->CopyFrom(finger_table_[i]);
    }
    //reply->mutable_finger_table() = finger_table;
    info("Ser: {} add req from: {}:{} pre node  before:{}, after: {}",cur_pos_, ip, port, pre_node_, req->pos());
    reply->set_pre_node(pre_node_);
    reply->set_pre_node_ip_port(pre_node_ip_port_);
    
    //create pre_CH_node_client first 

    //debug info:before return print info
    for(auto i=0;i<reply->finger_table_size();i++){
      auto& item = reply->finger_table(i);
      debug("Ser: addreq from {} item: start {}, successor {}, ip {}, ", pre_node_, item.start(), item.successor(), item.ip_port());
    }
    debug("Ser: reply finger table size {}", reply->finger_table_size());
    return {grpc::StatusCode::OK, ""};
  }
}

void CH_node_impl::push_increment_data(){
  do{
    usleep(push_data_interval_);
    if(next_pos_ == cur_pos_){
      continue;
    }

    auto& increment_data = kv_store_p_->get_increment_data();
    auto& backup_increment_data = kv_store_p_->get_backup_increment_data();
    std::unique_ptr<std::vector<std::string>> new_increment_data(new std::vector<std::string>());
    //swap(increment_data, new_increment_data);
    increment_data.swap(new_increment_data);
//    debug("Ser: backup data {} {}", kv_store_p_->get_backup_increment_data()->size(), new_increment_data->size());
//    debug("Ser: data {} {}", increment_data->size(), new_increment_data->size());

    auto& inner_cache = kv_store_p_->get_mutable_inner_cache();
    Bicache::PushDataRequest  pushDataReq;
    Bicache::PushDataReply pushDataRsp;
    pushDataReq.set_ip(cur_host_ip_);
    pushDataReq.set_ip(kv_port_);
    pushDataReq.set_pos(cur_pos_);
    std::shared_lock<std::shared_mutex> r_lock(kv_store_p_->get_inner_cache_lock());
    //备用队列（当上一次push请求失败时，这个队列内才会有数据
    for(auto key : *backup_increment_data){
      auto ite = inner_cache.find(key);
      if(ite != inner_cache.end()){
        pushDataReq.add_key(key);
        pushDataReq.add_value(ite->second.second);
        pushDataReq.add_expire_time(ite->second.first);
      }
    }
    for(auto key : *new_increment_data){
      auto ite = inner_cache.find(key);
      if(ite != inner_cache.end()){
        pushDataReq.add_key(key);
        pushDataReq.add_value(ite->second.second);
        pushDataReq.add_expire_time(ite->second.first);
      }
    }
    r_lock.unlock();
    pushDataReq.set_last_snapshot(cur_snapshot_for_main_);
    grpc::ClientContext ctx;
    auto status = CH_client_->PushIncrement(&ctx, pushDataReq, &pushDataRsp);
    if(status.ok()){
      cur_snapshot_for_main_++;
      backup_increment_data->clear();
      info("push {} keys to {} ip {} success, snapshot {}, now increment size {} ", new_increment_data->size(), next_pos_, next_node_ip_port_, cur_snapshot_for_main_, increment_data->size());
    }else{
      //rollback when failing to push,
      for(auto& key:*new_increment_data){
        backup_increment_data->emplace_back(key);
      }
      warn("push data to {} ip {} failed, snapshot {}, errmsg {}, backup incrementdata size {}, try again", next_pos_, next_node_ip_port_, cur_snapshot_for_main_, status.error_message(), backup_increment_data->size());
    }
  }while(true);
}

//接收来自上一个节点 push 的增量数据
//TODO::就目前只有两个接口（实际上只有一个set matters）来说，是可以不考虑重复的请求的，但如果说要支持 add、sub 等接口，就不能这么搞了
Status CH_node_impl::PushIncrement(::grpc::ServerContext* context, const ::Bicache::PushDataRequest* req, ::Bicache::PushDataReply* rsp){
  if(req->pos() != pre_node_){
    warn("Ser:PushIncrement with from {}, which is not my cai", req->pos());
    return {grpc::StatusCode::ABORTED, "you're not pre node"};
  }
  //只有一个set的话对比snapshot的意义并不大，这里增加对比选项，主要是为了扩展性
  if(req->last_snapshot() != cur_snapshot_for_backup_){
    warn("Ser:PushIncrement with mismatch snapshot id {} (cur {}) from {}", req->last_snapshot(), cur_snapshot_for_backup_, req->pos());
    return {grpc::StatusCode::ABORTED, "your snaphost id is identical with mine"};
  }
  auto& backup = kv_store_p_->get_backup();
  {
    auto curr_ts = get_miliseconds();
    auto expire_time = curr_ts;
    int valid_keys = 0;
    std::shared_lock w_lock(backup.lock);
    for(auto i = 0;i<req->key_size();i++){
      if(i%10==0){
        curr_ts = get_miliseconds();
      }
      expire_time = req->expire_time(i);
      if(expire_time == 0 || expire_time > curr_ts){
        auto ite= backup.backup_cache.find(req->key(i));
        if(ite==backup.backup_cache.end()){
          backup.backup_cache[req->key(i)]=std::make_pair(req->expire_time(i), req->value(i));
        }else{
          ite->second.first= req->expire_time(i);
          ite->second.second= req->value(i);
        }
        valid_keys++;
      }else{
        //debug("key {} value {} expire {}, FAILED TO ADD", getDataReply.key(i), getDataReply.value(i), getDataReply.expire_time(i));
      }
    }
    info("Ser: PushIncrement get {} valid keys from pre node({} in total), backup size {} snapshot {}", \
      valid_keys, req->key_size(), backup.backup_cache.size(), cur_snapshot_for_backup_);
  }
  cur_snapshot_for_backup_++;
  // backup 中的数据只有在前一个节点失联的时候才会
  return {grpc::StatusCode::OK, ""};
}

// 二次请求，这个请求里面完成数据的迁移 以及节点的确认加入
//这里要分成两部分，另外新开一个 AddNodeAck 接口
//为什么不直接搞事情呢？分出一个 二次 确认的逻辑是为什么呢？
//其实和三次握手的逻辑有点像
Status CH_node_impl::GetData(::grpc::ServerContext* context, const ::Bicache::GetDataRequest* req, ::Bicache::GetDataReply* reply){
    //TODO::如果要支持多个节点同时加入的话，这里需要进行判断可能会有多 AddNodeReq 进来，数据切分的时候可能会有问题
    //我们这里通过外部操作来保证一次只有一个节点加入
    auto ip = req->ip();
    auto port = req->port();
    auto pos = req->pos();
    auto pre_node_ip_port_tmp = ip+":"+port;
    //新加入节点点，得切分数据的状态
    if(req->join_node()){
      //标记系统状态
      SystemStatus.store(1);
      //TODO 先简单的实现，直接把数据遍历然后拆分一份
      //后面这里得是拆分成 2^mbit 个 shard 的形式
      auto& inner_map = kv_store_p_->split_inner_cache();
      //std::decay<decltype(inner_map)>::type pre_node_values;
      uint64_t key_hash_value;
      {
        std::shared_lock<std::shared_mutex> r_lock(kv_store_p_->get_inner_cache_lock());
        for(auto& val : inner_map){
          key_hash_value = MurmurHash64B(val.first.c_str(), val.first.length()) % virtual_node_num_;
          if(in_range(key_hash_value)  && !in_range(key_hash_value, pos)){
            //debug("key {} pos is {}: to outter", val.first, key_hash_value);;
            reply->add_key(val.first);
            reply->add_value(val.second.second);
            reply->add_expire_time(val.second.first);
          }else{
            //debug("key {} pos {} ISN’T ", val.first, key_hash_value);;
          }
        }
      }
      //发送数据
      if(pre_node_ip_port_tmp != cur_host_port_+":"+cur_host_port_){
        pre_CH_client_ = std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
          pre_node_ip_port_tmp, grpc::InsecureChannelCredentials()));
      }
      pre_node_ = req->pos();
      pre_node_ip_port_ = pre_node_ip_port_tmp;
      cur_snapshot_for_backup_ =1;
      pos2host_[pre_node_] = pre_node_ip_port_;
      sleep(1);
      SystemStatus.store(0);
    }else{
      //获取全部数据
      auto& inner_map = kv_store_p_->split_inner_cache();
      //获取写锁，来避免其他的数据进入
      std::shared_lock<std::shared_mutex> w_lock(kv_store_p_->get_inner_cache_lock());
      for(auto& val : inner_map){
        reply->add_key(val.first);
        reply->add_value(val.second.second);
        reply->add_expire_time(val.second.first);
      }
      w_lock.unlock();
//    这个分支的代码一般是下游节点请求上游的全部数据用的，所以不同reset for_backup变量
//    cur_snapshot_for_backup_ =1;
    }
    info("{} GetData from {}: send {} keys ", cur_pos_, req->pos(), reply->key_size());
    return {grpc::StatusCode::OK, ""};
}

Status CH_node_impl::FindPreDecessor(::grpc::ServerContext* context, const ::Bicache::FindPreDecessorRequest* req, ::Bicache::FindPreDecessorReply* reply){
  //调用 inner 的查找
  auto pos = req->pos();
  int close_one=0;
  //info("Ser: receive FP from {} for {}", req->node(),  pos);
  if(pos == cur_pos_){
      //直接找到自己了
      reply->set_found(true);
      reply->set_node(cur_pos_);
      reply->set_successor(cur_pos_);
      reply->set_succ_ip_port(cur_host_ip_ +":" +cur_host_port_);
      //info("Ser: Node {}: findpre {} from {}, found:{}, successor {}", cur_pos_, pos, req->node(), reply->found(), reply->successor());
      return {grpc::StatusCode::OK, ""};
  }
  // 这里也类似的逻辑，同样需要判断
  auto ret = find_closest_preceding_finger(pos, close_one, finger_table_);

  if(ret != -1){
      reply->set_found(true);
      reply->set_node(cur_pos_);
      reply->set_successor(ret);
      reply->set_succ_ip_port(pos2host_[ret]);
  }else{
      reply->set_found(false);
      reply->set_node(cur_pos_);
      //auto next_node_pos = (cur_pos_+1)%virtual_node_num_;
      //没有找到的时候，应该返回 close one 
      reply->set_successor(close_one);
      if(pos2host_.find(close_one)==pos2host_.end()){
        critical("Ser: {} can't find ip of {}", cur_pos_, close_one);
      }
      reply->set_succ_ip_port(pos2host_[close_one]);
  }
//  info("Ser: Node {}: findpre {} from {}, found:{}, successor {}", cur_pos_, pos, req->node(), reply->found(), reply->successor());
  return {grpc::StatusCode::OK, ""};
}

Status CH_node_impl::HeartBeat(::grpc::ServerContext* context, const ::Bicache::HeartBeatRequest* req, ::Bicache::HeartBeatReply* reply){
  //同时也要维护一个结构体，关于上下游节点的信息
  //节点密集加入的时候，finger_table 的 size 可能是会为0的，所以要加入判断逻辑
  if(finger_table_.empty()){
    reply->set_next_pos(next_pos_);
  }else{
    reply->set_next_pos(finger_table_.cbegin()->start());
  }
  reply->set_next_node_ip_port(next_node_ip_port_);
  reply->set_pre_pos(pre_node_);
  reply->set_pre_node_ip_port(pre_node_ip_port_);
  //info(cur_pos_, context->peer()+" get HB from "+ std::to_string(req->pos()));
  next_node_ip_port_ = req->host_ip()+":"+req->host_port();
  if(next_pos_!= req->pos()){
    info("get HB from different node: pre {}, after {}", next_pos_, req->pos());
    next_pos_ = req->pos();
    CH_client_ = std::move(std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
        next_node_ip_port_, grpc::InsecureChannelCredentials() )));
    cur_snapshot_for_main_ =1;
  }

  //这个地方可能会因为多线程的原因导致出错
  //如果 HB 来的比较早，这个地方可能会崩掉，所以最好还是一个个的节点的进行加入
  if(finger_table_.size()!=0){
    finger_table_[0].set_successor(next_pos_);
    finger_table_[0].set_ip_port(next_node_ip_port_);
  }
  //TODO::多线程操作加锁
  pos2host_[next_pos_] = next_node_ip_port_;
  //debug("HB from {} ipport {}", req->pos(), next_node_ip_port_);
  return {grpc::StatusCode::OK, ""};
}

int CH_node_impl::find_closest_preceding_finger(int pos, int& close_one, const std::vector<Bicache::FingerItem>& finger_table){
  int i = mbit-1;
  //首先判断在不在这个 finger_table 的范围内，不在的话，返回最后一个，最后一个如果是自己，则自己就是
  //论文里返回的节点 n，应该返回最远的
  if(finger_table.size()==0){
    critical("node: {} find closest preceding finger with NULL finger_table", cur_pos_);
    exit(1);
  }

  // 还是要多读文献。其实就工程来说，学术方面的能力可能不是那么重要
  auto finger_start = finger_table[0].start();

  for(;i>=0;i--){
    auto finger_pos = finger_table[i].successor();
    //there is an interval
    //printf("close : {} {} {}\n", finger_start, finger_pos, pos);
    //auto& item = finger_table[i];
    //printf("In find:node {}, finger {}, {}, {}, \n", cur_pos_, item.start(), item.interval(), item.successor());
    if(finger_start > finger_pos){
      if(pos<finger_start && pos> finger_pos){
        close_one = finger_pos;
        return -1;
      }
    }else{
      if(pos > finger_pos || pos < finger_start){
        close_one = finger_pos;
        return -1;
      }
    }
  }
  //
  close_one = cur_pos_;

  if(close_one == cur_pos_){
    //2种情况，一种在范围内，一种不在范围内
    // 相对来说比较差/丑的方式
    //可以参考 in_range 的实现，相对优雅一些
    if(next_pos_> cur_pos_){
      if(pos > cur_pos_ && pos <= next_pos_){
        //info("1debug: {} {} {}", cur_pos_, next_pos_, pos);
        return next_pos_;
      }else{
        return cur_pos_;
      }
    }else{
      if(pos > next_pos_ && pos <= cur_pos_ ){
        return cur_pos_;
      }else{
        return next_pos_;
      }
    }
  }
}

bool CH_node_impl::find_successor(int node, int pos, int& successor){
  //WARNING:: 这里需要注意，如果是在构建过程中的话，finger_table_ 可能还没有构建完成，使用可能会出core。
  if(pos == cur_pos_){
    successor = next_pos_;
    return true;
  }
  if(cur_pos_ == (pos +1)%virtual_node_num_){
    successor = cur_pos_;
    return true;
  }
  int close_one = -1;
  if(node == cur_pos_){
    //先找到最近的
    //debug("node {} cur_pos_ {} ", node, cur_pos_);
    auto ret = find_closest_preceding_finger(pos, close_one, finger_table_);
    if( ret != -1){
      successor = ret;
      return true;
    }
    //debug("{} close to {} is {}, ret = {}", node, pos, close_one, ret);
  }

  
  //创建 client
  if(close_one == -1){
    close_one = node;
  }
  auto ite_to_host = pos2host_.find(close_one);
  if(ite_to_host == pos2host_.end()){
    critical("{} no ip of pos {}", cur_pos_, close_one);
    return false;
  }
  auto node_ip_port = ite_to_host->second;
  ::Bicache::FindPreDecessorRequest req;
  ::Bicache::FindPreDecessorReply reply;
  int retry = 0;
  do{
    if(node_ip_port.empty()){
      critical("findPreClient ip is NULL, pos {}", close_one);
    }
    auto CH_client = std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
        node_ip_port, grpc::InsecureChannelCredentials()));
    req.set_pos(pos);
    req.set_node(cur_pos_);
    ::grpc::ClientContext ctx;
    auto status = CH_client->FindPreDecessor(&ctx, req, &reply);
    //防止对方节点失联
    if(!status.ok()){
      retry++;
      if(retry>=3){
        critical("{} find pre failed, pre&peer: {} {}", cur_pos_, pos, node_ip_port);
        return false;
      }
      usleep(500000);
    }
    if(reply.found()){
      successor = reply.successor();
      return true;
    }else{
      //TODO: 加上 node_pos
      info("find {} from {} ip {} failed, jump to node {} ip:port {}", pos, close_one, node_ip_port, reply.successor(), reply.succ_ip_port());
      close_one = reply.successor();
      node_ip_port = reply.succ_ip_port();

      auto ite = pos2host_.find(reply.successor());
      if(ite == pos2host_.end()){
        pos2host_.insert({reply.successor(), reply.succ_ip_port()});
      }else{
        ite->second = reply.succ_ip_port();
      }
      sleep(1);
    }
  }while(true);
}

const std::vector<Bicache::FingerItem>& CH_node_impl::get_finger_table()const{
  return finger_table_;
}

int CH_node_impl::add_node_req(){
  CH_client_ = std::move(std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
      next_node_ip_port_, grpc::InsecureChannelCredentials() )));

  Bicache::AddNodeRequest addNodeReq;
  addNodeReq.set_ip(cur_host_ip_);
  addNodeReq.set_port(cur_host_port_);
  addNodeReq.set_pos(cur_pos_);
  Bicache::AddNodeReply reply;
  grpc::ClientContext context;
  auto status = CH_client_->AddNode(&context, addNodeReq, &reply);
  if(status.ok()){
    //TODO:: 这里没有加锁，但这并不意味这里就是安全的，是要考虑的
    //这里可以确保 add_node_req 不是当前的节点发出的，所以可以安全的更新 finger_table
    //initialize the finger_table of newcomer node(this parts may be abstracted into a block/function of codes)

    std::vector<Bicache::FingerItem> finger_table;
    for(int i=0;i<reply.finger_table_size();i++){
      auto& item= reply.finger_table(i);
      debug("GET finger: node {}, finger {}, {}, {}, {}", cur_pos_, item.start(), item.interval(), item.successor(), item.ip_port().c_str());
      finger_table.push_back(std::move(*reply.mutable_finger_table(i)));
      auto ite = pos2host_.find(finger_table.back().successor());
      if(ite == pos2host_.end()){
        pos2host_.insert({finger_table.back().successor(), finger_table.back().ip_port()});
      }else{
        ite->second = finger_table.back().ip_port();
      }
    }
    auto interval_start = cur_pos_+1;
    for(int i=1;i<=mbit;i++){
      Bicache::FingerItem item;
      //TODO::to be accomplished
      item.set_start(interval_start);
      item.set_interval(pow(2.0, double(i-1)));

      // 左闭右开
      int item_upper = int(interval_start + pow(2.0, double(i-1)) - 1) % virtual_node_num_;
      //find the closest one 
      int close_one = 0;
      auto ret = find_closest_preceding_finger(item_upper, close_one, finger_table);
      int successor = 0;
      if(ret != -1){
        //进行直接拿到
        item.set_successor(ret);
      }else{
        debug("find_suc close_one {}", close_one);
        if(!find_successor(close_one, item_upper, successor)){
          critical("{} find successor {}  failed on {}", cur_pos_, item_upper, close_one);
        }
        item.set_successor(successor);
      }
      interval_start = (interval_start + item.interval())%virtual_node_num_;
      item.set_ip_port(pos2host_[item.successor()]);
      //TEST:查看 finger_table 的构建
      //info(cur_pos_, "fingeritem "+ std::to_string(item.start())+ std::to_string())
      info("BUILDING: node {}, finger {}, {}, {}, {}", cur_pos_, item.start(), item.interval(), item.successor(), item.ip_port().c_str());
      finger_table_.push_back(std::move(item));
      //TODO:: 这里要填充  pos2host
    }
    // 过滤逻辑：当下一个节点与当前的相同的时候
    // 与下一个节点建立连接
    // 从下一个节点获取数据
    Bicache::GetDataRequest getDataReq;
    getDataReq.set_ip(cur_host_ip_);
    getDataReq.set_port(cur_host_port_);
    getDataReq.set_pos(cur_pos_);
    getDataReq.set_join_node(true);
    Bicache::GetDataReply getDataReply;
    grpc::ClientContext ctx;
    auto status = CH_client_->GetData(&ctx, getDataReq, &getDataReply);
    // 这里应该是第一个 set pre_node_ip_port 的位置
    //get_writable_inner_cache
    {
      auto& inner_cache = kv_store_p_->get_mutable_inner_cache();
      auto curr_ts = get_miliseconds();
      uint64_t expire_time = 0;
      std::string key;
      std::string value;
      int valid_keys = 0;
      std::unique_lock<std::shared_mutex> w_lock(kv_store_p_->get_inner_cache_lock());
      for(auto i =0 ;i<getDataReply.key_size();i++){
        //每次都获取时间其实也是非常的耗时的，所以采用一种优化的方式
        if(i%10==0){
          curr_ts = get_miliseconds();
        }
        expire_time = getDataReply.expire_time(i);
        if(expire_time == 0 || expire_time > curr_ts){
          inner_cache[getDataReply.key(i)]=std::make_pair(getDataReply.expire_time(i), getDataReply.value(i));
          valid_keys++;
        }else{
          //debug("key {} value {} expire {}, FAILED TO ADD", getDataReply.key(i), getDataReply.value(i), getDataReply.expire_time(i));
        }
      }
      info("Ser:get {} valid keys from next node({} in total)", valid_keys, getDataReply.key_size());
    }
    notify_to_proxy();
    auto pre_node_ip_port_tmp = reply.pre_node_ip_port();
    if(!pre_node_ip_port_tmp.empty()){
      pre_CH_client_ = std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
        pre_node_ip_port_tmp, grpc::InsecureChannelCredentials()));
    }
    pre_node_ip_port_ = pre_node_ip_port_tmp;
    pre_node_ = reply.pre_node();
    info("{} pre pos: {} prenode:{} ", cur_pos_, pre_node_, pre_node_ip_port_);
    return 0; 
  }
  critical(status.error_message());
  return -1;
}



void CH_node_impl::run(){
  if(register_to_proxy(cur_host_ip_, cur_host_port_)){
    critical("register error, aborting");
    exit(-1);
  }
  //create client for next node 
  int retry = 0;
  do{
    if(next_node_ip_port_ == cur_host_ip_+":"+cur_host_port_){
      info("first node registered");
      break;
    }
    if(add_node_req()==0)
      break;
    if(retry ==2 ){
      critical("add node failed, aborting");
      exit(-1);
    }
    retry++;
  }while(retry<3);
}

void CH_node_impl::set_kv_store_p(KV_store_impl* kv_store){
  kv_store_p_ = kv_store;
}

void CH_node_impl::stablize(){
  // 这个线程是只会有一个线程在跑的，所以用了 static random engine, instead of thread_local
  std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, mbit-1);

  int count = 10;
  do{
    //这种每个循环都必须要执行的，就放在循环的前面好了
    usleep(stablize_interval_);
    uint32_t finger_num_to_fix = dis(gen);
    // 如果 stablize 过程中发生了节点的变更，发出 info log
    if(finger_num_to_fix < finger_table_.size() && finger_num_to_fix > 0 && next_pos_ != cur_pos_ ){
      auto& item = finger_table_[finger_num_to_fix];
      auto pre_succ = item.successor();
      auto successor = 0;
      //这里是有可能出错的，如果出错的话，是不应该把这个给搞进去的。
      if(!find_successor(cur_pos_, item.start(), successor)){
        critical("{} find successor {}  failed on {}", cur_pos_, item.start(), cur_pos_);
        continue;
      }
      item.set_successor(successor);
      item.set_ip_port(pos2host_[successor]);
      if(pre_succ != successor){
        info("stablize:node {}, finger {} changed from {} to {}, next_node {}", cur_pos_, item.start(), pre_succ, successor, next_pos_);
      }
      //TODO:: 访问最后一个参数的时候有可能出core，也就是说 string.c_str() 参数访问的时候是可能出core的
      if(10==count){
        info("stablize:node {}, finger {}, {}, {}, {}", cur_pos_, item.start(), item.interval(), item.successor(), item.ip_port());
        count=0;
      }
      count++;
    }
  }while(!exit_flag_);
}

void CH_node_impl::HB_to_proxy(){
  //Status ProxyServerImpl::HeartBeat(ServerContext* context, const ProxyHeartBeatRequest* req, ProxyHeartBeatReply* reply){
  ::Bicache::ProxyHeartBeatRequest req;
  ::Bicache::ProxyHeartBeatReply reply;
  int retry = 0;
  //retry_times to prenode
  int retry_time = 0;
  uint64_t sleep_time = 500000;
  const uint64_t origin_sleep_time = 500000;

  // HB to pre node 
  ::Bicache::HeartBeatRequest node_req;
  ::Bicache::HeartBeatReply node_reply;
  auto HB_to_pre_node = [&node_req, &node_reply, this]()->bool{
    node_req.set_pos(this->cur_pos_);
    node_req.set_host_port(this->cur_host_port_);
    node_req.set_host_ip(this->cur_host_ip_);
    ::grpc::ClientContext ctx;
    auto status = this->pre_CH_client_->HeartBeat(&ctx, node_req, &node_reply);
    if(!status.ok()){
      critical("{} connect to pre node failed, prenode: {}", this->cur_pos_,this->pre_node_);
      return false;
    }else{
      //this->node_status_[0].pos = this->pre_node_;
      //this->node_status_[0].status = NodeStatus::Alive;
      this->pp_pos_ = node_reply.pre_pos();
      this->pp_node_ip_port_ = node_reply.pre_node_ip_port();
      //info(cur_pos_, "HB to " + std::to_string(this->pre_node_));
      return true;
    }
  };
  while(!exit_flag_){
    if(pre_node_ != -1 &&pre_node_ != cur_pos_){
      bool success = HB_to_pre_node();
      if(success){
        retry_time = 0;
      }else{
        retry_time++;
        if(retry_time == 3){
          retry_time =0;
          info("HB to {} {} failed for 3 times, connect to pre pre one {} {}", pre_node_, pre_node_ip_port_, pp_pos_, pp_node_ip_port_);
          pre_node_ = pp_pos_;
          pre_node_ip_port_ = pp_node_ip_port_;
          //这时上一个节点已经发生了变化，应该通知上层

          //merge backup 数据
          //TODO::在心跳线程中 merge 数据可能会导致，心跳包超时，我这里超时就设置的大一些。
          auto& inner_cache = kv_store_p_->get_mutable_inner_cache();
          auto& backup_data = kv_store_p_->get_backup();
          std::shared_lock<std::shared_mutex> w_lock(kv_store_p_->get_inner_cache_lock());
          for(auto& val:backup_data.backup_cache){
            //理论上 backup_data 和 inner_cache 的数据不会相交，所以没有检测
            inner_cache.insert(std::make_pair(std::string(val.first), val.second));
          }
          backup_data.backup_cache.clear();
          info("clearing all the keys of backup");
          w_lock.unlock();

          if(pre_node_ == cur_pos_){
            info("find pre_node_ == cur_pos_, which means there are only one node");
            next_pos_ = cur_pos_;
            next_node_ip_port_ = cur_host_ip_ + ":" + cur_host_port_;
            //update finger_table 
            for(auto i=0;i<finger_table_.size();i++){
              finger_table_[i].set_successor(cur_pos_);
            }
          }else{
            //Get add data of pre
            Bicache::GetDataRequest getDataReq;
            getDataReq.set_ip(cur_host_ip_);
            getDataReq.set_port(cur_host_port_);
            getDataReq.set_pos(cur_pos_);
            getDataReq.set_join_node(false);
            Bicache::GetDataReply getDataReply;
            grpc::ClientContext ctx;
            auto status = CH_client_->GetData(&ctx, getDataReq, &getDataReply);
            if(!status.ok()){
              //失败了应该重试，我这里直接GG了
              critical("Ser: Get Data failed ");
            }
            for(auto i=0;i<getDataReply.key_size();i++){
              backup_data.backup_cache[getDataReply.key(i)] = std::make_pair(getDataReply.expire_time(i), getDataReply.value(i));
            }
            cur_snapshot_for_backup_ = 1;
            info("Ser: get {} keys from prenode because of LEAVING", getDataReply.key_size());
          }
          pre_CH_client_ = std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
            pre_node_ip_port_, grpc::InsecureChannelCredentials()));
        }
      }
    }
    //和 proxy & pre node 的通信放在同一个线程里面
    //keep HeartBeat with proxy
    ::grpc::ClientContext ctx;
    req.set_pos(cur_pos_);
    auto status = proxy_client_->HeartBeat(&ctx, req, &reply);
    //debug("HB to proxy");
    if(!status.ok()){
      debug("{} FAILED HB to proxy", cur_pos_);
    }
    usleep(sleep_time);

// 下面这些重试的逻辑先不要管了
//    if(!status.ok()){
//      critical(cur_pos_, std::to_string(retry)+ "try HB with proxy failed");
//      retry++;
//      if(retry>10){
//        sleep_time = 2000000;
//      }
//    }else{
//      retry = 0;
//      sleep_time = 300000;
//    }
  }
}

bool CH_node_impl::in_range(const uint32_t pos, const uint32_t begin_pos)const{
  // 利用 range 来进行判断
  auto start_pos = (begin_pos + 1)%virtual_node_num_; 
  if(cur_pos_ >= start_pos){
    if(pos >= start_pos &&pos<= cur_pos_){
      return true;
    }
  }else{
    if(pos >= start_pos || pos <= cur_pos_){
      return true;
    }
  }
  return false;
//  uint32_t range =( virtual_node_num_ + cur_pos_ - begin_pos + 1) % virtual_node_num_;
//  if(range == 0){
//    return true;
//  }
//  uint32_t actu_range =( virtual_node_num_ + cur_pos_ - pos + 1) % virtual_node_num_;
//  if(actu_range <= range){
//    return true;
//  }else{
//    return false;
//  }
  }

bool CH_node_impl::in_range(const uint32_t pos)const{
  // 利用 range 来进行判断
  auto start_pos = (pre_node_ + 1)%virtual_node_num_;
  if(cur_pos_ >= start_pos){
    if(pos >= start_pos &&pos<= cur_pos_){
      return true;
    }
  }else{
    if(pos >= start_pos || pos <= cur_pos_){
      return true;
    }
  }
  return false;
}

int CH_node_impl::get_pre_node()const{
  return pre_node_;
}
int CH_node_impl::get_pp_node()const{
  return pp_pos_;
}

int CH_node_impl::get_virtual_node_num_()const{
  return virtual_node_num_;
}

CH_node_impl::~CH_node_impl(){
  exit_flag_ = true;
  info("waiting for update thread stop");
  update_thr_->join();
  info("waiting for stablize thread stop");
  stablize_thr_->join();
  info("clear, byebye~");
}

// //保留关于其他节点的信息
// int virtual_node_num_;
// int cur_pos;
// std::string ip;
// std::string port;
