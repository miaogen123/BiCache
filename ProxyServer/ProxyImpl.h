#pragma once
#include "../pb/ProxyServer.grpc.pb.h"
#include "../MyUtils/cpp/hash.h"
#include "../MyUtils/cpp/fileOp.h"
#include <grpcpp/grpcpp.h>
#include <string>
#include <unordered_map>
#include <mutex>

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
private:
    //需要有数据结构，方便的记录当前的哈希环的拓扑：要考虑动态的删减的情况，
    //需要有 host 和位置，以及位置和 host 的映射
    std::unordered_map<std::string, int> host2pos_;
    std::map<int, std::string> pos2host_;
    //node register 需要是有序的
    static std::mutex add_node_lock_;

    int virtual_node_num_ = 10000;
};
}