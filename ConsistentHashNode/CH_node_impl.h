#pragma once
#include <string>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
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
using Bicache::AddNodeRequest;
using Bicache::AddNodeReply;

class CH_node_impl : public Bicache::ConsistentHash::Service {

public:
    //to communicate with proxyServer 

    CH_node_impl(Conf& conf);
    CH_node_impl()=delete;
    int register_to_proxy(const std::string& host, const std::string& port);
    Status AddNode(::grpc::ServerContext* context, const ::Bicache::AddNodeRequest* request, ::Bicache::AddNodeReply* reply)override;
    void run();

private:
    int add_node_req();

    //保留关于其他节点的信息
    int virtual_node_num_;
    // this mutex is uesd to protect the range
    std::mutex range_mu_;
    int range_start_ = 0;
    // cur_pos which is also range_end
    int cur_pos_;
    std::string next_ip_port_;
    std::string cur_host_ip_;
    std::string cur_host_port_;
    std::unique_ptr<Bicache::ProxyServer::Stub> proxy_client_;
    std::unique_ptr<Bicache::ConsistentHash::Stub> CH_client_;
};