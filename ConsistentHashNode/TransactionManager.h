#pragma once
#include <vector>
#include <map>
#include <string>

class SingleTransaction{
public:
int req_id;
std::vector<std::string> keys;
std::vector<std::string> values;
//不用记录之前的值，不存在回滚到之前的值的情况
//这里两阶段提交是：1. 锁定资源；2.提交修改。
//std::vector<std::string> pre_values;
//步骤执行超时
int step_time_out =0;
uint64_t expiera_at =0;
private:
};

//用一个map来记录超时
class TransactionManager{
public:

private:
std::map<int, SingleTransaction> transactoins;
};