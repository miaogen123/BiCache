#include <memory>
#include <cstdio>
#include <cmath>
#include <random>
#include <unistd.h>
#include "CH_node_impl.h"

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
  update_thr_ = std::make_shared<std::thread>(&CH_node_impl::HB_to_proxy, this);
  stablize_thr_ = std::make_shared<std::thread>(&CH_node_impl::stablize, this);
}

int CH_node_impl::register_to_proxy(const std::string& host, const std::string& port){
  RegisterRequest req;
  RegisterReply reply;
  req.set_ip(host);
  req.set_port(port);
  req.set_kv_port(kv_port_);
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

    auto pre_node_ip_port_tmp = ip+":"+port;
    if(pre_node_ip_port_tmp != cur_host_port_+":"+cur_host_port_){
      pre_CH_client_ = std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
        pre_node_ip_port_tmp, grpc::InsecureChannelCredentials()));
    }
    pre_node_ = req->pos();
    pre_node_ip_port_ = pre_node_ip_port_tmp;
    pos2host_[pre_node_] = pre_node_ip_port_;
    //debug info:before return print info
    for(auto i=0;i<reply->finger_table_size();i++){
      auto& item = reply->finger_table(i);
      //好像没有 copy 成功？
      debug("Ser: addreq from {} item: start {}, successor {}, ip {}, ", pre_node_, item.start(), item.successor(), item.ip_port());
    }
    debug("Ser: reply finger table size {}", reply->finger_table_size());
    return {grpc::StatusCode::OK, ""};
  }
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
      info("Ser: Node {}: findpre {} from {}, found:{}, successor {}", cur_pos_, pos, req->node(), reply->found(), reply->successor());
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
  info("Ser: Node {}: findpre {} from {}, found:{}, successor {}", cur_pos_, pos, req->node(), reply->found(), reply->successor());
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
  //info(cur_pos_, context->peer()+" get HB from "+ std::to_string(req->pos()));
  next_pos_ = req->pos();
  next_node_ip_port_ = req->host_ip()+":"+req->host_port();

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

//Status CH_node_impl::FindSuccessor(::grpc::ServerContext* context, const ::Bicache::FindSuccRequest* req, ::Bicache::FindSuccReply* reply){
//
//}

//TODO:: delete
//Status CH_node_impl::FindSuccessor(::grpc::ServerContext* context, const ::Bicache::FindSuccRequest* req, ::Bicache::FindSuccReply* reply){
//  int pos = req->pos();
//  //TODO:: 可以先做一个判断看是不是在当前的 range 之内，可以减少一些代价
////  if(){
////    reply->found = false;
////    return {grpc::StatusCode::Aborted, "not in my range"};
////  }
//  int ret= find_successor(pos, finger_table);
//  if(ret > 0){
//    reply->set_found(true);
//    reply->set_successor(ret);
//    return {grpc::StatusCode::OK, ""};
//  }
//  reply->set_successor((- ret)%virtual_node_num_);
//  reply->set_found(false);
//  return {grpc::StatusCode::ABORTED, "not in my range"};
//}
//
//int CH_node_impl::find_successor(int pos, std::vector<Bicache::FingerItem>& finger_table){
//  for(int i=mbit-1; i>=0;i--){
//    auto& item = finger_table[i-1];
//    if( ((pos + virtual_node_num_) - item.start())% virtual_node_num_ < item.interval()){
//      return pos;
//    }
//  }
//  if(finger_table[mbit-1].successor() ==0 ){
//    return -virtual_node_num_;
//  }
//  return - finger_table[mbit-1].successor();
//}

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
      if(pos >= finger_pos || pos < finger_start){
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
    auto ret = find_closest_preceding_finger(pos, close_one, finger_table_);
    if( ret != -1){
      successor = ret;
      return true;
    }
    info("{} close to {} is {}, ret = {}", node, pos, close_one, ret);
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
    // 与上一个节点建立连接
    // 这里应该是第一个 set pre_node_ip_port 的位置
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

void CH_node_impl::stablize(){
  // 这个线程是只会有一个线程在跑的，所以用了 static random engine, instead of thread_local
  std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, mbit-1);

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
      info("stablize:node {}, finger {}, {}, {}, {}", cur_pos_, item.start(), item.interval(), item.successor(), item.ip_port());
    }
  }while(!exit_flag_);
}

void CH_node_impl::HB_to_proxy(){
  //Status ProxyServerImpl::HeartBeat(ServerContext* context, const ProxyHeartBeatRequest* req, ProxyHeartBeatReply* reply){
  ::Bicache::ProxyHeartBeatRequest req;
  ::Bicache::ProxyHeartBeatReply reply;
  int retry = 0;
  uint64_t sleep_time = 500000;

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
      this->node_status_[0].pos = this->pre_node_;
      this->node_status_[0].status = NodeStatus::Alive;
      //info(cur_pos_, "HB to " + std::to_string(this->pre_node_));
      return true;
    }
  };
  while(!exit_flag_){
    if(pre_node_ != -1 &&pre_node_ != cur_pos_){
      HB_to_pre_node();
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

bool CH_node_impl::in_range(const uint32_t pos)const{
  // 利用 range 来进行判断
  uint32_t range =(next_pos_ + virtual_node_num_ - cur_pos_) % virtual_node_num_;
  if(range == 0){
    return true;
  }
  uint32_t actu_range =(pos + virtual_node_num_ - cur_pos_) % virtual_node_num_;
  if(actu_range <= range){
    return true;
  }else{
    return false;
  }
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
