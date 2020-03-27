#pragma once
#include <string>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <thread>
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

    void init();
    void run();
    ~KV_store_impl();
    // 需要对外提供一个查询 successor 的接口

private:
    //CH_node
    void run_CH_node();
    Conf inner_conf_;
    std::shared_ptr<CH_node_impl> ch_node_;
    std::shared_ptr<std::thread> CH_node_serving_thr_;
};