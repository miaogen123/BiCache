## on the way project. not serving until everythings is ok
## 不随commit更新，最后搞完再整理
***
# BiCache
这个是我毕业设计做的分布式缓存，所以叫做Bi(毕）Cache（狗头保命）。
- 设计POINTS
    - 底层协议使用chord，上层使用二级hash的方式做KV，同时支持分布式事务
    
- grpc 安装好了设置CmakeList中的 GRPC_PATH 变量
