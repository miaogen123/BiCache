
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "../pb/ProxyServer.grpc.pb.h"
#include "../utils/log.h"
#include "../utils/conf.h"
#include "../utils/host.h"
#include "CH_node_impl.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;

int main() {
  spdlog::set_pattern("[%H:%M:%S:%e] [%^%l%$] [tid %t] %v");
  Conf conf("CH_config");
  if(!conf.get_file_stated()){
    spdlog::critical("can't find conf file");
    return -1;
  }
  std::string host_name;
  std::string ip;
  int ret = get_host_info(host_name, ip);
  if(ret){
    spdlog::critical("get hostip error");
    exit(-1);
  }
  INFO(ip);
  conf.set("host_ip", ip);
  std::string port = conf.get("CH_node_port");
  if(port.size()==0){
    spdlog::critical("get port error");
    exit(-1);
  }
  std::string server_address("0.0.0.0:"+port);
  CH_node_impl node{conf};

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&node);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("CH node listening on {}", server_address);

//  std::thread server_ack(grpc::Server::Wait, &server);
//  server_ack.detach();
  node.run();
  server->Wait();
  return 0;
}
