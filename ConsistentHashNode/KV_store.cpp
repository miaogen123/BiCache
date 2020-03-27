#include <memory>
#include <cstdio>
#include <cmath>
#include <random>
#include <unistd.h>
#include "KV_store.h"

KV_store_impl::KV_store_impl(Conf& conf):inner_conf_(conf){
  
}

void KV_store_impl::init(){
  CH_node_serving_thr_ = std::make_shared<std::thread>(&KV_store_impl::run_CH_node, this);
}

//单独的线程用来服务
void KV_store_impl::run_CH_node(){
  std::string CH_node_port = inner_conf_.get("CH_node_port");
  if(CH_node_port.size()==0){
    spdlog::critical("get CH_node_port error");
    exit(-1);
  }
  
  std::string server_address("0.0.0.0:"+CH_node_port);
  ch_node_ = std::make_shared<CH_node_impl>(inner_conf_);
  //CH_node_impl node{inner_conf};
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(ch_node_.get());
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::info("CH node listening on {}", server_address);
  ch_node_->run();
  server->Wait();
}

void KV_store_impl::run(){

}

KV_store_impl::~KV_store_impl(){
  CH_node_serving_thr_->join();
}