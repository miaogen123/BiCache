#pragma once
#include <initializer_list>

void ERROR(const std::string& str);
void INFO(const std::string& str);

void ERROR(const int& node, const std::string& str);
void INFO(const int& node, const std::string& str);

void ERROR(const int& node, std::initializer_list<std::string> str_list);
void INFO(const int& node, std::initializer_list<std::string> str_list);