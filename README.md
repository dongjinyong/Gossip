Gossip
======

Java实现的Gossip协议，借鉴CassandraV1.1.1源码

运行方法：
Config类中的3个ip换成一个虚拟机中3个虚拟网卡的ip，
DatabaseDescriptor类中的seed的ip换成其中一个网卡的ip
或者：
Config类中的3个listen_adress都用本机同一个ip，端口换成3个不同的端口，
DatabaseDescriptor类中的seed的adress换成其中一个ip:port

启动
CassandraDaemon1
CassandraDaemon2
CassandraDaemon3
即可。
其中CassandraDaemon1比较特殊，包括定时更新applicationState数据，使onChange事件触发。