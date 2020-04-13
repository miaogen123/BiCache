#include <algorithm>
#include <unistd.h>
#include "ProxyImpl.h"
#include <chrono>
#include <shared_mutex>
#include "../utils/log.h"

namespace Bicache{

using grpc::StatusCode;

//静态成员初始化
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
    auto ite_req_time_out= conf.find("req_time_out");
    if(ite_req_time_out !=conf.end()){
        req_time_out = std::atoi(ite_req_time_out->second.c_str());
    }else{
        req_time_out = 5;
    }
    auto ite_auto_commit_time_out= conf.find("auto_commit_time_out");
    if(ite_auto_commit_time_out!=conf.end()){
        auto_commit_time_out= std::atoi(ite_req_time_out->second.c_str());
    }else{
        auto_commit_time_out= 10;
    }
    update_flag_ = true;
    info("proxy server working");
    //std::thread* tmp = new std::thread(&ProxyServerImpl::backend_update, this);
    update_thr_ = std::make_shared<std::thread>(&ProxyServerImpl::backend_update, this);
}

uint32_t ProxyServerImpl::get_key_successor(const std::string& key, uint32_t& key_pos){
  auto hash_value = MurmurHash64B(key.c_str(), key.length());
  key_pos = hash_value % virtual_node_num_;
  std::unique_lock<std::mutex> w_lock(add_node_lock_);
  auto ite_lower = pos2host_.lower_bound(key_pos);
  uint32_t successor = 0;
  if(ite_lower == pos2host_.end()){
    successor = pos2host_.cbegin()->first;
  }else{
    successor = ite_lower->first;
  }
  return successor;
}

Status ProxyServerImpl::Register(ServerContext* context, const RegisterRequest* req, RegisterReply* reply){
    auto ip = req->ip();
    auto port = req->port();
    auto ip_port_origin = ip + ":" + port;
    auto ip_port_hash = ip_port_origin;
    int pos = 0;
    int retry_count = 0;
    do{
        if(req->pos()!=-1){
            pos = req->pos();
            pos2host_.insert({pos, ip_port_origin});
            pos2kvhost_.insert({pos, ip+":"+req->kv_port()});
            host2pos_.insert({ip_port_origin, pos});
            break;
        }
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
                //放在前面 req->pos != -1 的时候里面了，因为节点只有在拿到数据以后才能加入到 proxy， 
            //    pos2host_.insert({pos, ip_port_origin});
            //    pos2kvhost_.insert({pos, ip+":"+req->kv_port()});
            //    host2pos_.insert({ip_port_origin, pos});
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
        if(pos2host_.size()==0){
            reply->set_next_node_ip_port(ip_port_origin);
            reply->set_next_node_pos(pos);
        }else{
            auto ite_first_pos = pos2host_.cbegin();
            reply->set_next_node_ip_port(ite_first_pos->second);
            reply->set_next_node_pos(ite_first_pos->first);
        }
    }
    //reply->set_next_node_pos(ite_upper->first);
    reply->set_total_range(virtual_node_num_);
    reply->set_pos(pos);
    
    info("receive register: type: {}, {}:{}, get pos:{} next pos {}, next host {}",req->pos()==-1?"ToGetPos":"AlreadyGetData", ip, port, pos,  reply->next_node_pos(), reply->next_node_ip_port());

    //record the node
    if(req->pos()!=-1){
        uint64_t seconds = get_miliseconds()+3000;
        pos_HB_lock_.lock();
        auto ite_pos = pos_HB_.find(pos);
        if(ite_pos==pos_HB_.end()){
            pos_HB_.insert({pos, seconds});
        }else{
            ite_pos->second = seconds;
        }
        pos_HB_lock_.unlock();
        {
            //超时也不删除，因为他终究是会被删掉的
            std::unique_lock<std::mutex> w_lock(lock_for_client_);
            auto CH_client = std::make_shared<Bicache::KV_service::Stub>(grpc::CreateChannel(
               ip+":"+req->kv_port(), grpc::InsecureChannelCredentials()));
            pos2client_.insert({pos, CH_client});
            debug("node {} register at {} with {} create client", pos, seconds, ip_port_origin);
        }
    }
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
    //debug("update pos {} to cur_seconds {}", pos,cur_seconds);
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
    reply->set_virtual_node_num(virtual_node_num_);
    info("client getConfig from {}, host size {}", context->peer(), pos2kvhost_.size());
    return grpc::Status{grpc::StatusCode::OK, ""};
}

Status ProxyServerImpl::Transaction(ServerContext* context, const TransactionRequest* req, TransactionReply* reply){
    //希望多个值，要么进行修改，要么就全部回滚
    //不给支持设置超时
    debug("get transaction...");
    std::vector<int> pos_list;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for(auto i=0;i<req->keys_size();i++){
        keys.push_back(req->keys(i));
        values.push_back(req->values(i));
        //这里得找到对应的pos，让客户端来做也行
        uint32_t key_pos = 0;
        auto pos = get_key_successor(keys.back().c_str(), key_pos);
        pos_list.push_back(pos);
    }
    //把相关key都上锁
    std::map<int, std::vector<int>> pos_keys;
    {
        //对需要的key等待1ms，超过5ms没有拿到所有锁就离开
        int count = 5;
        std::unique_lock<std::mutex> w_lock(lock_for_transaction_keys_);
        bool lock_flag = true;
        w_lock.unlock();
        for(auto i = 0 ;i<keys.size();i++){
            int tmp_pos = pos_list[i];
            pos_keys[tmp_pos].push_back(i);
            auto& key = keys[i];
            //debug("get {} key {} ", i, key);
            do{
                if(lock_flag){
                    w_lock.lock();
                    lock_flag=false;
                }
                auto ite= keys_occupied_.find(key);
                if(ite==keys_occupied_.end()){
                    keys_occupied_.insert(key);
                    //debug("insert key {} ", key);
                    break;
                }else{
                    //释放锁,sleep,重试
                    w_lock.unlock();
                    lock_flag = true;
                    count--;
                    //debug("insert failed key {} count {}", key, count);
                    if(count == 0 ){
                        debug("req id {} get keys lock failed", req->req_id());
                        return {grpc::StatusCode::ABORTED, "get keys lock timeout"};
                    }
                }
                usleep(sleep_interval_in_locking_keys_);
            }while(true);
        }
    }

    bool success_flag = true;
    int step_count = 0;
    do{
        //拿到key锁，开始协调事务，node要lock对应的资源，调用各个node的prepare方法 
        //这里为了方便就全部串行了
        //prepare
        //获取client
        //构造请求
        std::map<int, TransactionStepRequest> step_reqs;
        std::map<int, TransactionStepReply> step_rsps;
        std::map<int, KV_client_ptr> clients;
        {
            std::unique_lock<std::mutex> w_lock(lock_for_client_);
            for(auto& pair :pos_keys){
                TransactionStepRequest step_req;
                auto ite = pos2client_.find(pair.first);
                if(ite==pos2client_.end()){
                    //debug("transaction can't find client of {}", pair.first);
                    return grpc::Status{grpc::StatusCode::ABORTED, "can't find client of pos " + std::to_string(pair.first)};
                }else{
                    if(clients.find(pair.first)==clients.end()){
                        //debug("add client for {} ", pair.first);
                        clients[pair.first]=ite->second;
                    }
                }
                for(auto& pos:pair.second){
                    step_req.add_keys(keys[pos]);
                    step_req.add_values(values[pos]);
                }
                //TODO::step_id使用enum class 会更加合适
                step_req.set_step_id(1);
                step_req.set_req_id(req->req_id());
                step_reqs.insert({pair.first, std::move(step_req)});
                step_rsps.insert({pair.first, TransactionStepReply{}});
            }
        }
        //序列化调用 prepare 接口，当然更好的方式是并行
        for(auto& pair:clients){
            auto& client = pair.second; 
            int pos = pair.first;
            //这里是确定 req 是一定存在的
            auto& step_req = step_reqs[pos];
            step_req.set_step_id(1);
            step_req.set_req_time_out(req_time_out);
            step_req.set_auto_commit_time_out(auto_commit_time_out);
            auto& step_rsp = step_rsps[pos];
            grpc::ClientContext ctx;
            auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(req_time_out);
            ctx.set_deadline(deadline);

            auto status = client->TransactionPrepare(&ctx, step_req, &step_rsp);
            if(!status.ok()){
                success_flag= false;
                std::string extra_msg("");
                if(status.error_code() == grpc::UNIMPLEMENTED ){
                    extra_msg="unimplemented";
                }
                debug("reqid {} step {} errormsg {} extra {}", req->req_id(), step_count, status.error_message(), extra_msg);
                break;
            }
        }
        debug("req_id {} transacton prepare finished", req->req_id());
        step_count++;
        if(!success_flag){
            //rollback
            for(auto& pair:clients){
                auto& client = pair.second; 
                int pos = pair.first;
                //这里是确定 req 是一定存在的
                auto& step_req = step_reqs[pos];
                step_req.set_step_id(3);
                auto& step_rsp = step_rsps[pos];
                grpc::ClientContext ctx;
                auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(req_time_out);
                ctx.set_deadline(deadline);
                //直接回滚
                //TODO::这里可以改成异步或者并行分发的情况
                auto status = client->TransactionRollback(&ctx, step_req, &step_rsp);
            }
            debug("req_id {} transacton rollbacked ", req->req_id());
        }else{
            //commit 
            int count =0;
            for(auto& pair:clients){
                auto& client = pair.second; 
                int pos = pair.first;
                //这里是确定 req 是一定存在的
                auto& step_req = step_reqs[pos];
                step_req.set_step_id(2);
                step_req.set_req_time_out(req_time_out);
                step_req.set_auto_commit_time_out(auto_commit_time_out);
                auto& step_rsp = step_rsps[pos];

                grpc::ClientContext ctx;
                auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(req_time_out);
                ctx.set_deadline(deadline);

                auto status = client->TransactionCommit(&ctx, step_req, &step_rsp);
                if(!status.ok()){
                    success_flag= false;
                    debug("reqid {} step {} errormsg {}", req->req_id(), step_count, status.error_message());
                    break;
                }
                count++;
            }
            if(count !=clients.size()){
                critical("NOT ALL TRANSACTIONS COMMITTED, PLEASE CHECK THIS TRANSACTION MANUALLY");
            }
            debug("req_id {} transacton committed", req->req_id());
        }
    }while(false);
    
    //结束，释放对于key的锁
    {
        std::unique_lock<std::mutex> w_lock(lock_for_transaction_keys_);
        for(auto& key:keys){
            //auto ite= keys_occupied.find(key);
            keys_occupied_.erase(key);
        }
        debug("release keys in transaction");
    }
    debug("req {} finish at step count {}", req->req_id(), step_count);
    if(success_flag){
        return {grpc::StatusCode::OK, "get keys lock timeout"};
    }else{
        return {grpc::StatusCode::ABORTED, "error at phase "+std::to_string(step_count)};
    }
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
                    warn("from proxy erase node {}: current time {}, node register time {}", host_ptr->first, seconds, ite->second);
                }
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

}//end Bicache

std::mutex Bicache::ProxyServerImpl::add_node_lock_;