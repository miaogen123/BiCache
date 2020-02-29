#include <memory>
#include <cmath>
#include "CH_node_impl.h"

//to communicate with proxyServer 

CH_node_impl::CH_node_impl(Conf& conf){
  //init proxyClient
  auto proxy_ip = conf.get("proxy_ip");
  if(proxy_ip.size() == 0){
      ERROR("whthout proxy ip, service starts failed");
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
}

int CH_node_impl::register_to_proxy(const std::string& host, const std::string& port){
  RegisterRequest req;
  RegisterReply rsp;
  req.set_ip(host);
  req.set_port(port);
  ClientContext context;
  auto status = proxy_client_->Register(&context, req, &rsp);
  if(status.ok()){
    cur_pos_ = rsp.pos();
    next_ip_port_ = rsp.next_node_ip_port();
    virtual_node_num_ = rsp.total_range();
    if(next_ip_port_ == host+":"+port){
      range_start_ = (cur_pos_ + 1)%virtual_node_num_;
      //initialize first finger table
      auto interval_start = range_start_;
      for(int i=1;i<=mbit;i++){
        Bicache::FingerItem item;
        item.set_start(interval_start);
        item.set_interval(pow(2.0, double(i-1)));
        item.set_successor(cur_pos_);
        item.set_ip_port(cur_host_ip_+":"+cur_host_port_);
        interval_start = (interval_start + item.interval())%virtual_node_num_;
        finger_table.push_back(std::move(item));
      }
      pos2host[cur_pos_]=cur_host_ip_+":"+cur_host_port_;
    }
    std::string msg = "register OK, "+ std::to_string(range_start_) +" "+ std::to_string(rsp.pos()) + " " \
      + std::to_string(rsp.total_range()) +":"+ rsp.next_node_ip_port();
    INFO(msg.c_str());
    return 0;
  }else{
    ERROR("something is false, ");
    ERROR(status.error_message().c_str());
    return -1;
  }
}

// addnode 的操作必须要是序列化的
Status CH_node_impl::AddNode(::grpc::ServerContext* context, const ::Bicache::AddNodeRequest* req, ::Bicache::AddNodeReply* reply){
  auto ip = req->ip();
  auto port = req->port();
  auto pos = req->pos();
  {
    std::unique_lock<std::mutex> un_lock(range_mu_);
    if(range_start_ < cur_pos_ &&(pos<range_start_ || pos >cur_pos_ )){
      reply->set_status(-1);
      return {grpc::StatusCode::ABORTED, "your pos is not in my responsibility, find another one"};
    } else if(range_start_ >= cur_pos_&& (pos< cur_pos_ && pos > range_start_)){
      reply->set_status(-1);
      return {grpc::StatusCode::ABORTED, "your pos is not in my responsibility, find another one"};
    }
    if(cur_pos_ - range_start_ ==1){
      reply->set_status(-1);
      return {grpc::StatusCode::ABORTED, "my range has been limited to 1"};
    }
    //TODO:: 这里切分需要注意是原子性的，不能自己范围切了然后对方没有接受到数据。所以需要等对方把数据给切分了以后发送过来一个ack包，自己才停止对外服务
    //这个地方是有一个中间状态的，在这个中间状态需要不对外提供写服务，只提供读服务。
    reply->set_range_start(range_start_);
    //TODO:: 似乎这里的赋值都只能使用遍历？？？脑阔痛
    for(int i=0;i<finger_table.size();i++){
      reply->add_finger_table()->CopyFrom(finger_table[i]);
    }
    //reply->mutable_finger_table() = finger_table;
    range_start_ = (pos +1)% virtual_node_num_;
    INFO("add req from:"+ip+":"+port +" :before "+ std::to_string(reply->range_start()) +", after:"+std::to_string(range_start_));
    pre_node_ = req->pos();
    pre_node_ip_port_ = ip+":"+port;
    return {grpc::StatusCode::OK, ""};
  }
}
Status CH_node_impl::FindSuccessor(::grpc::ServerContext* context, const ::Bicache::FindSuccRequest* req, ::Bicache::FindSuccReply* reply){
  int pos = req->pos();
  //TODO:: 可以先做一个判断看是不是在当前的 range 之内，可以减少一些代价
//  if(){
//    reply->found = false;
//    return {grpc::StatusCode::Aborted, "not in my range"};
//  }
  int ret= find_successor(pos, finger_table);
  if(ret > 0){
    reply->set_found(true);
    reply->set_successor(ret);
    return {grpc::StatusCode::OK, ""};
  }
  reply->set_successor((- ret)%virtual_node_num_);
  reply->set_found(false);
  return {grpc::StatusCode::ABORTED, "not in my range"};
}

int CH_node_impl::find_successor(int pos, std::vector<Bicache::FingerItem>& finger_table){
  for(int i=mbit-1; i>=0;i--){
    auto& item = finger_table[i-1];
    if( ((pos + virtual_node_num_) - item.start())% virtual_node_num_ < item.interval()){
      return pos;
    }
  }
  if(finger_table[mbit-1].successor() ==0 ){
    return -virtual_node_num_;
  }
  return - finger_table[mbit-1].successor();
}


int CH_node_impl::add_node_req(){
  CH_client_ = std::move(std::make_unique<Bicache::ConsistentHash::Stub>(grpc::CreateChannel(
      next_ip_port_, grpc::InsecureChannelCredentials() )));

  Bicache::AddNodeRequest addNodeReq;
  addNodeReq.set_ip(cur_host_ip_);
  addNodeReq.set_port(cur_host_port_);
  addNodeReq.set_pos(cur_pos_);
  Bicache::AddNodeReply reply;
  grpc::ClientContext context;
  auto status = CH_client_->AddNode(&context, addNodeReq, &reply);
  if(status.ok()){
    //TODO:: 这里没有加锁，但这并不意味这里就是安全的，是要考虑的
    range_start_ =reply.range_start();
    //这里可以确保 add_node_req 不是当前的节点发出的，所以可以安全的更新 finger_table
    //initialize the finger_table of newcomer node(this parts may be abstracted into a block/function of codes)

    auto interval_start = range_start_;
    std::vector<Bicache::FingerItem> finger_table;
    for(int i=0;i<reply.finger_table_size();i++){
      finger_table.push_back(std::move(*reply.mutable_finger_table(i)));
    }
    for(int i=0;i<mbit;i++){
      Bicache::FingerItem item;
      //TODO::to be accomplished
      item.set_start(interval_start);
      item.set_interval(pow(2.0, double(i-1)));
      auto item_upper = interval_start + pow(2.0, double(i-1));
      //find the closest one 
      auto ret = find_successor(item_upper, finger_table);
      if(ret>0){
        //进行直接拿到
      }else{

      }
//      interval_start = (interval_start + item.interval())%virtual_node_num_;
//      finger_table.push_back(std::move(item));
    }

    return 0; 
  }
  ERROR(status.error_message());
  return -1;
}



void CH_node_impl::run(){
  
  if(register_to_proxy(cur_host_ip_, cur_host_port_)){
    ERROR("register error, aborting");
    exit(-1);
  }
  //create client for next node 
  int retry = 0;
  do{
    if(next_ip_port_ == cur_host_ip_+":"+cur_host_port_){
      INFO("first node registered");
      break;
    }
    if(add_node_req()==0)
      break;
    if(retry ==2 ){
      ERROR("add node failed, aborting");
      exit(-1);
    }
    retry++;
  }while(retry<3);
}

// //保留关于其他节点的信息
// int virtual_node_num_;
// int cur_pos;
// std::string ip;
// std::string port;