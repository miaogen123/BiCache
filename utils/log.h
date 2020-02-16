#pragma once
#include<iostream>

void ERROR(const char* str){
    std::cout <<"[ERROR]: "<< str << std::endl;
}

void INFO(const char* str){
    std::cout <<"[INFO]: "<< str << std::endl;
}