#include <iostream>
#include "log.h"

void ERROR(const std::string& str){
    std::cout <<"[ERROR]: "<< str << std::endl;
}

void INFO(const std::string& str){
    std::cout <<"[INFO]: "<< str << std::endl;
}