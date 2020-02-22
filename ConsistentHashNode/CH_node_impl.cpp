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
  stub_ = std::move(std::make_unique<Bicache::ProxyServer::Stub>(grpc::CreateChannel(
      proxy_ip + ":" + proxy_port, grpc::InsecureChannelCredentials() )));
  //TODO:: get local function
  cur_host_ip = conf.get("host_ip");
  cur_host_port = conf.get("CH_node_port");
}

int CH_node_impl::register_to_proxy(const std::string& host, const std::string& port){
  RegisterRequest req;
  RegisterReply rsp;
  req.set_ip(host);
  req.set_port(port);
  ClientContext context;
  auto status = stub_->Register(&context, req, &rsp);
  if(status.ok()){
    cur_pos = rsp.pos();
    next_ip_port = rsp.next_node_ip_port();
    virtual_node_num_ = rsp.total_range();
    std::string msg = "register OK, " + std::to_string(rsp.pos()) + " " + std::to_string(rsp.total_range()) +":"+ rsp.next_node_ip_port();
    INFO(msg.c_str());
    return 0;
  }else{
    ERROR("something is false, ");
    ERROR(status.error_message().c_str());
    return -1;
  }
}

void CH_node_impl::run(){
  register_to_proxy(cur_host_ip, cur_host_port);
}

//    //保留关于其他节点的信息
//    int virtual_node_num_;
//    int cur_pos;
//    std::string ip;
//    std::string port;