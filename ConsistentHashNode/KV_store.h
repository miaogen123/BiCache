#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <shared_mutex>

#include "../pb/KV_store.grpc.pb.h"
#include "CH_node_impl.h"
#include "../utils/log.h"
#include "../utils/conf.h"
#include "CH_node_impl.h"
#include "spdlog/spdlog.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::Status;
using grpc::ServerBuilder;

using spdlog::debug;
using spdlog::info;
using spdlog::warn;
using spdlog::critical;


class KV_store_impl : public Bicache::KV_service::Service {
public:
    //to communicate with proxyServer 
    KV_store_impl(Conf& conf);
    KV_store_impl()=delete;

    virtual ::grpc::Status Get(::grpc::ServerContext* context, const ::Bicache::GetRequest* request, ::Bicache::GetReply* response)override;

    void init();
    static void run();
    void backend_update();
    ~KV_store_impl();
    // 需要对外提供一个查询 successor 的接口

private:
    //CH_node
    void run_CH_node();
    Conf inner_conf_;

    // 后台线程在清理这个的时候，需要读写锁，不然可能会出core
    // TODO::这里有一个优化的地方，就是批量删除，遇到需要删除的值，可以把 ite 放进 vector 最后再删除。
    // 当然删除的时候是要做标记的，免得有请求过来的时候，写了相同的key，清理线程无感直接把这个给删除了，这样就不一致了
    std::shared_mutex rw_lock_for_ids_;
    std::unordered_map<uint32_t, uint64_t> req_ids_;
    //后面可以尝试 junction 仓库
    std::shared_mutex rw_lock_for_cache_;
    std::unordered_map<std::string, std::string> inner_cache_;
    //后台更新清理线程:两个任务，清理 req_ids_,清理 inner_cache_
    bool exit_clean_flag_;
    uint clean_interval_= 2000000;
    std::shared_ptr<std::thread> backend_update_thr_;
    //CH_node
    std::shared_ptr<CH_node_impl> ch_node_;
    std::shared_ptr<std::thread> CH_node_serving_thr_;
};