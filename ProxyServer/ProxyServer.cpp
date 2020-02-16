#include <iostream>
#include <unordered_map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "../MyUtils/cpp/fileOp.h"
#include "../pb/ProxyServer.grpc.pb.h"
#include "ProxyImpl.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;

void RunServer(const std::string& port, std::unordered_map<std::string, std::string>& conf) {
//void RunServer() {
  //std::unordered_map<std::string, std::string> conf;
  //std::string port;
  std::string server_address("0.0.0.0:" + port);
  Bicache::ProxyServerImpl service(conf);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Proxy Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {

  std::unordered_map<std::string, std::string> conf;
  std::string port;
  
  GetConfig(conf, "ProxyConfig");
  auto ite_to_port = conf.find("port");
  if( ite_to_port != conf.end()){
    port = (*ite_to_port).second;
  }
  //RunServer();
  std::thread detached_server(RunServer, port, std::ref(conf));
  detached_server.join();

  return 0;
}
