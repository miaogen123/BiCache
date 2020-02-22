
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "../pb/ProxyServer.grpc.pb.h"
#include "../utils/log.h"
#include "../utils/conf.h"
#include "../utils/host.h"
#include "CH_node_impl.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;

int main(int argc, char** argv) {
  Conf conf("CH_config");
  if(!conf.get_file_stated()){
    ERROR("can't find conf file");
    return -1;
  }
  std::string host_name;
  std::string ip;
  int ret = get_host_info(host_name, ip);
  if(ret){
    ERROR("get hostip error");
  }
  conf.set("host_ip", ip);
  CH_node_impl node{conf};
  node.run();
  return 0;
}
