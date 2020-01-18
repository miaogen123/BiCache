#include <iostream>
#include <unordered_map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "build/ConsistentHash.grpc.pb.h"
#endif

#include "MyUtils/cpp/fileOp.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using Bicache::ConsistentHash;
using Bicache::HeartBeatRequest;
using Bicache::HeartBeatReply;

class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(ConsistentHash::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string heartbeat(const std::string& port) {
    // Data we are sending to the server.
    HeartBeatRequest request;
    request.set_timestamp(atoi(port.c_str()));

    // Container for the data we expect from the server.
    HeartBeatReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->HeartBeat(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return std::to_string(reply.timestamp());
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<ConsistentHash::Stub> stub_;
};

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public ConsistentHash::Service {
  Status HeartBeat(ServerContext* context, const HeartBeatRequest* request,
                  HeartBeatReply* reply) override {
    std::string prefix("Hello ");
    reply->set_timestamp(time(0));
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  GreeterServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void GetConfig(std::unordered_map<std::string, std::string>& conf, const std::string& filename){
  auto fileContent = getAllOfFile(filename);
  std::vector<std::string> splited_content;
  SplitString(fileContent, splited_content, "\n");
  std::vector<std::string> tmp_vec;
  for(auto& part: splited_content){
    SplitString(part, tmp_vec, " ");
    conf[tmp_vec[0]]=tmp_vec[1];
    tmp_vec.clear();
  }
}

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  std::unordered_map<std::string, std::string> conf;
  std::string port;
  GetConfig(conf, "config");
  auto ite_to_port = conf.find("port");
  if( ite_to_port != conf.end()){
    port = (*ite_to_port).second;
  }
  std::thread detached_server(RunServer);
  detached_server.detach();
  sleep(3);
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:"+port, grpc::InsecureChannelCredentials()));
  std::string reply = greeter.heartbeat(port);
  std::cout << "Greeter received: " << reply << std::endl;
  sleep(3);

  return 0;
}
