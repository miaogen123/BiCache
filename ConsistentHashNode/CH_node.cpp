
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "../utils/conf.h"
#include "../utils/host.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "KV_store.h"

#define SPDLOG_DEBUG_ON

int main() {
  //spdlog::set_level(spdlog::level::info);
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S:%e] [%^%L%$] [tid %t] %v");
  Conf conf("CH_config");
  if(!conf.get_file_stated()){
    spdlog::critical("can't find conf file");
    return -1;
  }

  std::string host_name;
  std::string ip;
  int ret = get_host_info(host_name, ip);
  if(ret){
    spdlog::critical("get hostip error");
    exit(-1);
  }
  conf.set("host_ip", ip);
  KV_store_impl kv_store{conf};
  kv_store.init();
  return 0;
}