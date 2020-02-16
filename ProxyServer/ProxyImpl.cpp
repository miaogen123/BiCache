#include <algorithm>
#include "ProxyImpl.h"
#include "../utils/log.h"

namespace Bicache{

using grpc::StatusCode;

//静态成员初始化
std::mutex ProxyServerImpl::add_node_lock_;

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
                pos2host_.insert({pos, ip_port_origin});
                host2pos_.insert({ip_port_origin, pos});
                break;
            }
        }
    } while (true);
    // get next node;
    auto ite_upper = pos2host_.upper_bound(pos);
    if(ite_upper != pos2host_.end()){
        reply->set_next_node_ip_port(ite_upper->second);
    } else {
        auto ite_first_pos = pos2host_.cbegin();
        reply->set_next_node_ip_port(ite_first_pos->second);
    }
    reply->set_total_range(virtual_node_num_);
    reply->set_total_range(virtual_node_num_);
    reply->set_pos(pos);
    return Status::OK;
} 
    ////需要有数据结构，方便的记录当前的哈希环的拓扑：要考虑动态的删减的情况，
    ////需要有 host 和位置，以及位置和 host 的映射
    //std::unordered_map<std::string, int> host2pos;
    //std::unordered_map<int, std::string> pos2host;
    ////node register 需要是有序的
    //std::mutex add_node_lock;
    
}