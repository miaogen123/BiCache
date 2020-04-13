#pragma once
#include "../pb/ProxyServer.grpc.pb.h"
#include "../MyUtils/cpp/hash.h"
#include "../MyUtils/cpp/fileOp.h"
#include "../pb/KV_store.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <shared_mutex>
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

using KV_client = Bicache::KV_service::Stub;
using KV_client_ptr = std::shared_ptr<KV_client>;
using PosClientType = std::unordered_map<uint32_t, KV_client_ptr>;

class ProxyServerImpl final: public ProxyServer::Service{
    //return the next node and the position of its
public:
    ProxyServerImpl() = delete;
    ProxyServerImpl(std::unordered_map<std::string, std::string>& conf);
    Status Register(ServerContext* context, const RegisterRequest* req, RegisterReply* reply)override;
    Status HeartBeat(ServerContext* context, const ProxyHeartBeatRequest* req, ProxyHeartBeatReply* reply)override;
    Status GetConfig(ServerContext* context, const GetConfigRequest* req, GetConfigReply* reply)override;
    Status Transaction(ServerContext* context, const TransactionRequest* req, TransactionReply* reply)override;

    uint32_t get_key_successor(const std::string& key, uint32_t& key_pos);
    void backend_update();
    ~ProxyServerImpl();
private:
    //需要有数据结构，方便的记录当前的哈希环的拓扑：要考虑动态的删减的情况，
    //需要有 host 和位置，以及位置和 host 的映射

    std::mutex lock_for_client_;
    PosClientType pos2client_;
    
    uint32_t sleep_interval_in_locking_keys_;
    //keep lock for keys in transaction
    std::uint32_t req_time_out;
    std::uint32_t auto_commit_time_out;
    std::mutex lock_for_transaction_keys_;
    std::set<std::string> keys_occupied_;

    std::unordered_map<std::string, int> host2pos_;
    std::map<int, std::string> pos2host_;
    //pos2kvpost_ 和 pos2host_：前者是 pos->kv port
    std::map<int, std::string> pos2kvhost_;
    //node register 需要是有序的
    static std::mutex add_node_lock_;

    std::mutex pos_HB_lock_;
    std::unordered_map<int, uint64_t> pos_HB_;
    bool update_flag_;
    std::shared_ptr<std::thread> update_thr_;
    int virtual_node_num_ = 10000;
};
}