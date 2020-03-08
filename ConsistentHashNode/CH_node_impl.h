#pragma once
#include <string>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <thread>
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

    //Status FindSuccessor(::grpc::ServerContext* context, const ::Bicache::FindSuccRequest* request, ::Bicache::FindSuccReply* reply)override;
    Status FindPreDecessor(::grpc::ServerContext* context, const ::Bicache::FindPreDecessorRequest* request, ::Bicache::FindPreDecessorReply* reply)override;
    // run 里面做一些必要的开始工作
    int find_successor(int pos);
    void run();
    ~CH_node_impl();
    // 需要对外提供一个查询 successor 的接口

private:
    void HB_to_proxy();
    int add_node_req();
    bool find_successor(int pos, int node, int& successor);
    bool find_closest_preceding_finger(int pos, int& close_one, std::vector<Bicache::FingerItem>& finger_table);
    std::shared_ptr<std::thread> update_thr_;
    bool exit_flag_;
    //保留关于其他节点的信息
    int virtual_node_num_;
    int pre_node_;
    std::string pre_node_ip_port_;
    // this mutex is uesd to protect the range
    int cur_pos_;
    int mbit;
    std::string next_ip_port_;
    std::string cur_host_ip_;
    std::string cur_host_port_;
    std::unique_ptr<Bicache::ProxyServer::Stub> proxy_client_;
    std::unique_ptr<Bicache::ConsistentHash::Stub> CH_client_;
    std::vector<Bicache::FingerItem> finger_table_;
    std::unordered_map<uint, std::string> pos2host_;
};