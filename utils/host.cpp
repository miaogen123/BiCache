#include "host.h"
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <vector>
#include <iostream>

// get inner net ip
bool GetIP(const std::vector<std::string>& vNetType, std::string& strip)
{
    for(size_t i=0;i<vNetType.size();i++)
    {
        for(char c='0';c<='0';++c)
        {
            std::string strDevice = vNetType[i] + c; //根据网卡类型，遍历设备如eth0，eth1
            int fd;
            struct ifreq ifr;
            //使用UDP协议建立无连接的服务
            fd = socket(AF_INET, SOCK_DGRAM, 0);
            strcpy(ifr.ifr_name, strDevice.c_str() );       
            //获取IP地址
            if (ioctl(fd, SIOCGIFADDR, &ifr) <  0)
            {
                ::close(fd);
                continue;
            }

            // 将一个IP转换成一个互联网标准点分格式的字符串
            strip = inet_ntoa(((struct sockaddr_in*)&(ifr.ifr_addr))->sin_addr);
            if(!strip.empty())
            {
                ::close(fd);
                return true;
            }
        }
    }
    return false;
}

int get_host_info(std::string& hostname, std::string& ip){
    std::vector<std::string> vs;//预先定义了几种可能的网卡类型
    vs.push_back("eth");
    if(GetIP(vs, ip))
        return 0;
    return -1;
    
}
