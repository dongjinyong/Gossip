Gossip
======

Java实现的Gossip协议，代码大部分来自于CassandraV1.1.1源码，把Cassandra中Gossip协议相关部分抽取了出来，供大家参考。
目录结构与cassandra源码一致，以下为目录说明：
concurrent:并发包工具类
config:配置信息
gms:gossip的核心实现类
io:
locator:
net:通信相关，采用传统的BIO方式，可根据项目需要换成自己的通信服务层
service:供外部调用的方法，gossip协议使用方法实例程序
utils:




