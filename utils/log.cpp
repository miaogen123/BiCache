#include <iostream>
#include "log.h"

void ERROR(const std::string& str){
    std::cout <<"[ERROR]: "<< str << std::endl;
}

void INFO(const std::string& str){
    std::cout <<"[INFO]: "<< str << std::endl;
}

void ERROR(const int& node, const std::string& str){
    std::cout <<"[ERROR]: "<<"Node" << node <<" "<< str << std::endl;
}
void INFO(const int& node, const std::string& str){
    std::cout <<"[INFO]: "<<"Node"<< node<<" "<< str << std::endl;
}

void ERROR(const int& node,std::initializer_list<std::string> str_list){
    std::cout <<"[ERROR]: "<<"Node" << node <<" ";
    for(auto ite = str_list.begin();ite!=str_list.end();ite++){
        std::cout<<*ite<<" ";
    }
    std::cout<<std::endl;
}
void INFO(const int& node,std::initializer_list<std::string> str_list){
    std::cout <<"[INFO]: "<<"Node" << node <<" ";
    for(auto ite = str_list.begin();ite!=str_list.end();ite++){
        std::cout<<*ite<<" ";
    }
    std::cout<<std::endl;
}