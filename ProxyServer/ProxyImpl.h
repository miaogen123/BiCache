#pragma once
#include "../pb/ProxyServer.grpc.pb.h"
#include "../MyUtils/cpp/hash.h"
#include "../MyUtils/cpp/fileOp.h"
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <mutex>
#include "spdlog/spdlog.h"

using spdlog::info;
using spdlog::critical;
using spdlog::debug;
using spdlog::warn;

namespace Bicache{

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class ProxyServerImpl final: public ProxyServer::Service{
    //return the next node and the position of its
public:
    ProxyServerImpl() = delete;
    ProxyServerImpl(std::unordered_map<std::string, std::string>& conf);
    Status Register(ServerContext* context, const RegisterRequest* req, RegisterReply* reply)override;
    Status HeartBeat(ServerContext* context, const ProxyHeartBeatRequest* req, ProxyHeartBeatReply* reply)override;

    void backend_update();
    ~ProxyServerImpl();
private:
    //需要有数据结构，方便的记录当前的哈希环的拓扑：要考虑动态的删减的情况，
    //需要有 host 和位置，以及位置和 host 的映射
    std::unordered_map<std::string, int> host2pos_;
    std::map<int, std::string> pos2host_;
    //node register 需要是有序的
    static std::mutex add_node_lock_;

    std::mutex pos_HB_lock_;
    std::unordered_map<int, uint64_t> pos_HB_;
    bool update_flag_;
    std::shared_ptr<std::thread> update_thr_;
    int virtual_node_num_ = 10000;
};
}