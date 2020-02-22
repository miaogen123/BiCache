#pragma once
#include <string>
#include <grpcpp/grpcpp.h>
#include <memory>
#include "../pb/ConsistentHash.grpc.pb.h"
#include "../pb/ProxyServer.grpc.pb.h"
#include "../utils/log.h"
#include "../utils/conf.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;

class CH_node_impl : public Bicache::ConsistentHash::Service {

public:
    //to communicate with proxyServer 

    CH_node_impl(Conf& conf);
    CH_node_impl()=delete;
    int register_to_proxy(const std::string& host, const std::string& port);
    void run();

private:
    //保留关于其他节点的信息
    int virtual_node_num_;
    int cur_pos;
    std::string next_ip_port;
    std::string cur_host_ip;
    std::string cur_host_port;
    std::unique_ptr<Bicache::ProxyServer::Stub> stub_;
};