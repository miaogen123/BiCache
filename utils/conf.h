#pragma once
#include <iostream>
#include <string>
#include <vector>
#include "../MyUtils/cpp/fileOp.h"

class Conf{
public:
    Conf(const std::string& filename){
        auto fileContent = getAllOfFile(filename);
        if(fileContent.size()==0){
            is_file_opened = false;
        }else{
            parse(fileContent);
        }
    }
    Conf(std::ifstream& fstream){
        assert(false);
    }
    void parse(const std::string& fileContent){
        std::vector<std::string> splited_content;
        SplitString(fileContent, splited_content, '\n');
        std::vector<std::string> tmp_vec;
        for(auto& part: splited_content){
            SplitString(part, tmp_vec, ' ');
            inner_map[tmp_vec[0]]=tmp_vec[1];
            tmp_vec.clear();
        }
    }
    std::string get(const std::string& key, const std::string& default_value=""){
        auto ite = inner_map.find(key);
        if(ite == inner_map.end()){
            return default_value;
        }else{
            return ite->second;
        }
    }

    bool get_file_stated()const{
        return is_file_opened;
    }
    void set(const std::string& key, const std::string& value){
        inner_map.insert({key, value});
    }
private:
    bool is_file_opened = true;
    std::unordered_map<std::string, std::string> inner_map;
};