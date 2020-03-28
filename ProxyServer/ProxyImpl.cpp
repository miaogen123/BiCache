#include <algorithm>
#include <unistd.h>
#include "ProxyImpl.h"
#include <chrono>
#include "../utils/log.h"

namespace Bicache{

using grpc::StatusCode;

//静态成员初始化
std::mutex ProxyServerImpl::add_node_lock_;
uint64_t get_miliseconds(){
    using namespace std::chrono;
    steady_clock::duration d;
    d = steady_clock::now().time_since_epoch();
    return duration_cast<milliseconds>(d).count();
}

ProxyServerImpl::ProxyServerImpl(std::unordered_map<std::string, std::string>& conf){
    auto ite = conf.find("virtualNodeNumber");
    if ( ite == conf.end()) {
        //这里为了方便没有使用专门的 LOG 模块
        ERROR("get virtualNodeNumber failed from config file, use default value 10000");
    } else {
        auto virtual_node_num = std::atoi(ite->second.c_str());
        if (virtual_node_num == 0){
            ERROR("get virtualNodeNumber failed from config file, use default value 10000");
        }else{
            virtual_node_num_ = virtual_node_num;
        }
    }
    update_flag_ = true;
    info("proxy server working");
    //std::thread* tmp = new std::thread(&ProxyServerImpl::backend_update, this);
    update_thr_ = std::make_shared<std::thread>(&ProxyServerImpl::backend_update, this);
}

Status ProxyServerImpl::Register(ServerContext* context, const RegisterRequest* req, RegisterReply* reply){
    auto ip = req->ip();
    auto port = req->port();
    auto ip_port_origin = ip + ":" + port;
    auto ip_port_hash = ip_port_origin;
    int pos = 0;
    int retry_count = 0;
    do{
        retry_count++;
        pos = MurmurHash64B(ip_port_hash.c_str(), ip_port_hash.length()) % virtual_node_num_;
        {
            std::unique_lock<std::mutex> uni_lock(add_node_lock_);
            auto ite_host = pos2host_.find(pos);
            if( ite_host != pos2host_.end()){
                if(retry_count>10){
                    std::string fatal_error_msg("failed to add this node to 10 times, maybe you need to enlarge the virtual node num");
                    ERROR(fatal_error_msg.c_str());
                    return {grpc::StatusCode::ABORTED, fatal_error_msg};
                }
                ip_port_hash += port;
                continue;
            } else {
                //FIXME:::已经存在的节点，重新加入时，应该如何处理
                pos2host_.insert({pos, ip_port_origin});
                pos2kvhost_.insert({pos, ip+":"+req->kv_port()});
                host2pos_.insert({ip_port_origin, pos});
                break;
            }
        }
    } while (true);
    // get next node;
    auto ite_upper = pos2host_.upper_bound(pos);
    if(ite_upper != pos2host_.end()){
        reply->set_next_node_ip_port(ite_upper->second);
        reply->set_next_node_pos(ite_upper->first);
    } else {
        auto ite_first_pos = pos2host_.cbegin();
        reply->set_next_node_ip_port(ite_first_pos->second);
        reply->set_next_node_pos(ite_first_pos->first);
    }
    //reply->set_next_node_pos(ite_upper->first);
    reply->set_total_range(virtual_node_num_);
    reply->set_pos(pos);
    
    info("receive register: {}:{}, get pos:{} next pos {}, next host {}", ip, port, pos,  reply->next_node_pos(), reply->next_node_ip_port());

    //record the node
    uint64_t seconds = get_miliseconds()+3000;
    pos_HB_lock_.lock();
    auto ite_pos = pos_HB_.find(pos);
    if(ite_pos==pos_HB_.end()){
        pos_HB_.insert({pos, seconds});
    }else{
        ite_pos->second = seconds;
    }
    debug("node {} register at {}", pos, seconds);
    pos_HB_lock_.unlock();
    return Status::OK;
} 

//需要有数据结构，方便的记录当前的哈希环的拓扑：要考虑动态的删减的情况，
Status ProxyServerImpl::HeartBeat(ServerContext* context, const ProxyHeartBeatRequest* req, ProxyHeartBeatReply* reply){
    auto pos = req->pos();
    uint64_t cur_seconds = get_miliseconds()+3000;
    pos_HB_lock_.lock();
    auto ite_pos = pos_HB_.find(pos);
    if(ite_pos!=pos_HB_.end()){
        ite_pos->second = cur_seconds;
    }
    pos_HB_lock_.unlock();
    return grpc::Status{grpc::StatusCode::OK, ""};
}

Status ProxyServerImpl::GetConfig(ServerContext* context, const GetConfigRequest* req, GetConfigReply* reply){
    //老实讲，这个地方并发访问也是要加锁的，就是不知道三个线程用同一个把锁对于性能会不会有什么的地方
    {
        std::lock_guard<std::mutex> lg(pos_HB_lock_);
        for(auto& pair:pos2kvhost_){
            reply->add_pos_list(pair.first);
            reply->add_ip_port_list(pair.second);
        }

    }
    return grpc::Status{grpc::StatusCode::OK, ""};
}

void ProxyServerImpl::backend_update(){
    uint64_t seconds;
    while(update_flag_){
        sleep(1);
        seconds = get_miliseconds();
        info("start to clean unlinked node");
        //printf("current time %ld", seconds);
        pos_HB_lock_.lock();
        auto ite = pos_HB_.begin();
        auto ite_end = pos_HB_.end();
        for(;ite!=pos_HB_.end();){
            if(ite->second < seconds){
                add_node_lock_.lock();
                auto host_ptr = pos2host_.find(ite->first);
                if(host_ptr!=pos2host_.end()){
                    host2pos_.erase(host_ptr->second);
                    pos2kvhost_.erase(host_ptr->first);
                    pos2host_.erase(host_ptr);
                }
                warn("from proxy erase node {}: current time {}, node register time {}", host_ptr->first, seconds, ite->second);
                add_node_lock_.unlock();
                ite=pos_HB_.erase(ite);
            }else{
                ite++;
            }
        }
        pos_HB_lock_.unlock();
    }
}

ProxyServerImpl::~ProxyServerImpl(){
    update_flag_ = false;
    update_thr_->join();
}

}