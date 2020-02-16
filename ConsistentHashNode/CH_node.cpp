
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "../pb/ProxyServer.grpc.pb.h"
#include "../utils/log.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;


class ProxyClient {
 public:
  ProxyClient(std::shared_ptr<Channel> channel)
      : stub_(ProxyServer::NewStub(channel)) {}

  int register_to_Proxy() {
    RegisterRequest req;
    RegisterReply rsp;
    req.set_ip("0.0.0.0");
    req.set_port("7789");
    ClientContext context;
    auto status = stub_->Register(&context, req, &rsp);
    if(status.ok()){
      std::string msg = "register OK, " + std::to_string(rsp.pos()) + " " + std::to_string(rsp.total_range()) + rsp.next_node_ip_port();
      INFO(msg.c_str());
      return 0;
    }else{
    ERROR("something is false, ");
    ERROR(status.error_message().c_str());
    return -1;
    }
  }

 private:
  std::unique_ptr<ProxyServer::Stub> stub_;
};
int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  ProxyClient proxyClient(grpc::CreateChannel(
      "localhost:7790", grpc::InsecureChannelCredentials()));
  int reply = proxyClient.register_to_Proxy();
  return 0;
}
