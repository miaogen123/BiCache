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

enum class NodeStatus{
    Alive = 1,
    AliveCannotConnect,
    Disconnected
};

struct nodeStatus{
    int pos;
    NodeStatus status;
};

class CH_node_impl : public Bicache::ConsistentHash::Service {
public:
    //to communicate with proxyServer 

    CH_node_impl(Conf& conf);
    CH_node_impl()=delete;

    Status AddNode(::grpc::ServerContext* context, const ::Bicache::AddNodeRequest* request, ::Bicache::AddNodeReply* reply)override;
    //Status FindSuccessor(::grpc::ServerContext* context, const ::Bicache::FindSuccRequest* request, ::Bicache::FindSuccReply* reply)override;
    Status FindPreDecessor(::grpc::ServerContext* context, const ::Bicache::FindPreDecessorRequest* request, ::Bicache::FindPreDecessorReply* reply)override;
    //接收来自下游的信息（下游主动和上游进行通信）
    Status HeartBeat(::grpc::ServerContext* context, const ::Bicache::HeartBeatRequest* request, ::Bicache::HeartBeatReply* response);

    int register_to_proxy(const std::string& host, const std::string& port);
    int find_successor(int pos);
    // run 里面做一些必要的开始工作
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
    int pre_node_=-1;
    std::string pre_node_ip_port_;
    // this mutex is uesd to protect the range
    int cur_pos_;
    int mbit;
    int next_pos_;
    std::string next_node_ip_port_;
    std::string cur_host_ip_;
    std::string cur_host_port_;
    std::unique_ptr<Bicache::ProxyServer::Stub> proxy_client_;
    std::unique_ptr<Bicache::ConsistentHash::Stub> CH_client_;
    // 在跟上一个节点发生了 HB 以后在进行 pre node client 的创建
    std::unique_ptr<Bicache::ConsistentHash::Stub> pre_CH_client_;
    std::vector<Bicache::FingerItem> finger_table_;
    std::unordered_map<uint, std::string> pos2host_;
    // 记录前一个和后一个节点的状态(0和1)
    // 这个跟之前的 pre_nnode_ next_node 多少有点重合，之后有精力的话再进行重构吧 TODO::
    std::vector<nodeStatus> node_status_{{-1, NodeStatus::Disconnected}, {-1, NodeStatus::Disconnected}};
};