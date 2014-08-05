MiniCubes
=========

MiniCubes是一个高性能、分布式、内存型OLAP计算引擎。设计上利用Java8 Stream的高性能计算特性来支撑单JVM节点上的聚集计算，基于Hazelcast#Distributed ExecutorService来做分布式计算。

我们采用了Spring Boot来构建“自包含”的应用。

模块列表如下：
* minicubes-core：提供基础的数据容器和聚合计算API
* minicubes-cluster：提供分布式聚合计算能力。
* minicubes-lucene：TODO
