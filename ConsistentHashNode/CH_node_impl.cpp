#include <memory>
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
    range_start_ = (pos +1)% virtual_node_num_;
    INFO("add req from:"+ip+":"+port +" :before "+ std::to_string(reply->range_start()) +", after:"+std::to_string(range_start_));
    return {grpc::StatusCode::OK, ""};
  }
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