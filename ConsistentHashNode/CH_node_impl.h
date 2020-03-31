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
#include "spdlog/spdlog.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using Bicache::ProxyServer;
using Bicache::RegisterRequest;
using Bicache::RegisterReply;
using Bicache::AddNodeRequest;
using Bicache::AddNodeReply;
using spdlog::info;
using spdlog::critical;
using spdlog::debug;
using spdlog::warn;

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

    //for upper service
    bool in_range(const uint32_t pos)const;
    int find_closest_preceding_finger(int pos, int& close_one, const std::vector<Bicache::FingerItem>& finger_table);
    const std::vector<Bicache::FingerItem>& get_finger_table()const;
private:
    void stablize();
    void HB_to_proxy();
    int add_node_req();
    bool find_successor(int pos, int node, int& successor);


    //上层服务的信息
    std::string kv_port_;
    //config 
    int stablize_interval_ = 1000000;
    bool exit_flag_ = false;
    //保留关于其他节点的信息
    int mbit;
    int virtual_node_num_;
    //这个命名也多少有点不一致
    int pre_node_=-1;
    std::string pre_node_ip_port_;
    int next_pos_;
    std::string next_node_ip_port_;
    int cur_pos_;
    std::string cur_host_ip_;
    std::string cur_host_port_;
    
    //记录N个上节点，记录的节点越多，可靠性也就越高
    //TODO::后面有扩展的想法在搞这个吧，我这里就用简单的上一个节点好了。
    //std::vector<int> pre_node_list;
    //std::vector<std::string> pre_node_ipport_list;
    int pp_pos_;
    std::string pp_node_ip_port_;
    std::unique_ptr<Bicache::ProxyServer::Stub> proxy_client_;
    std::unique_ptr<Bicache::ConsistentHash::Stub> CH_client_;
    // 在跟上一个节点发生了 HB 以后在进行 pre node client 的创建
    std::unique_ptr<Bicache::ConsistentHash::Stub> pre_CH_client_;
    std::vector<Bicache::FingerItem> finger_table_;
    std::unordered_map<uint, std::string> pos2host_;
 
    //后台线程
    std::shared_ptr<std::thread> stablize_thr_;
    std::shared_ptr<std::thread> update_thr_;
    // 记录前一个和后一个节点的状态(0和1)
    // 这个跟之前的 pre_nnode_ next_node 多少有点重合，之后有精力的话再进行重构吧 TODO::
    std::vector<nodeStatus> node_status_{{-1, NodeStatus::Disconnected}, {-1, NodeStatus::Disconnected}};
};