Java实现的Gossip协议，基于CassandraV1.1.1源码修改实现（尽量保留了Cassandra源码），把Cassandra中Gossip协议相关部分抽取了出来，供大家参考。
目录结构与cassandra源码一致，以下为目录说明：
concurrent:并发包工具类.
config:配置信息.
gms:gossip的核心实现类.
io:序列化相关.
locator:事件监听器和seed接口.
net:通信相关，采用传统的BIO方式，可根据项目需要换成自己的通信服务层.
service:供外部调用的方法，gossip协议使用方法实例程序.
utils:工具类.

对Cassandra源码的修改主要包括：
1. InetAddress换成InetSocketAddress，可以为每个实例指定不同的端口，方便本机启用多个实例调试。
2. 去掉了与cassandra业务耦合的一些类和方法，对监听器接口做了简化。
3. 简化通信部分，如果项目需要，或者有独立的通信服务代码，直接替换net目录下的类或接口即可。


运行方法：
1. 修改相关配置：
Config类中的3个ip换成一个虚拟机中3个虚拟网卡的ip，
DatabaseDescriptor类中的seed的ip换成其中一个网卡的ip
或者：
Config类中的3个listen_adress都用本机同一个ip，端口换成3个不同的端口，
DatabaseDescriptor类中的seed的adress换成其中一个ip:port

2. run以下三个类:
CassandraDaemon1
CassandraDaemon2
CassandraDaemon3
即可。（相当于三个server节点）
其中CassandraDaemon1比较特殊，负责定时更新applicationState数据，使onChange事件触发，其它节点均可捕获到这一变化。
