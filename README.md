MiniCubes
=========

MiniCubes是一个轻量级、高性能、分布式、内存型OLAP计算引擎（利用Java8 Stream的高性能计算特性来支撑单JVM节点上的聚集计算），提供聚合函数有：
* sum：指定指标的SUM聚集计算。
* groupby；指定指标在某一维度上的GROUPBY聚合计算。
* distinct[-count]：指定指标/维度在某一维度上的DISTINCT和DISTINCT-COUNT计算。
**上述函数均支持分布式计算特性**

MiniCubes设计上追求极致简单，代码量只有2k行左右，我们认为：
* 对于大部分聚合计算来说，单机单线程是最快的。

## 数据：
* Java8 Stream parallel平均会快于sequential，但parallel模式稳定度不如sequential。
    1. 循环执行几次sum聚集计算，parallel模式的第一次通常较慢，大概是后面几次的5倍左右；
* 8G内存中大约可以存放2.5kw条记录（5个维度和4个指标），这些原始数据大小应该在1.5G。
* 3台物理机+1台虚拟机搭建集群上载入大概10kw条数据：
    1. 单指标全量sum耗时平均400ms左右；
    2. 单指标全量groupby-sum耗时平均600ms左右，结果7000条左右；
    3. 单指标全量groupby-distinct耗时平均800ms左右，结果7000条左右；
    4. 单指标全量groupby-distinctcount耗时平均800ms左右，结果7000条左右；
    5. 虚拟机性能是物理机的1/3~1/2,所以请保持虚拟机载入数据量为物理机1/3~1/2；
* 我并没有发现Java8 Stream的[Concurrent Reduction](http://docs.oracle.com/javase/tutorial/collections/streams/parallelism.html#concurrent_reduction "Concurrent Reduction")会更快。
**以上基于CPU12核/内存128G的物理机和CPU8核/16G内存搭建的集群测试得出**

## 本地开发：
我们采用Spring Boot来构建“自包含”的应用，JDK1.8，Maven3：
* minicubes-core目录下执行：mvn clean install -DskipTests=true
* minicubes-cluster目录下执行：mvn clean install -DskipTests=true
* Maven仓库目录下：com/github/totyumengr/minicubes-cluster/目录下，找到jar包
* 直接执行：java -server -jar minicubes-cluster-VERSIONS.jar
* 访问：http://localhost:PORT/[status,reassign,sum]

*可以修改src/main/resources/application.properties来改变配置。也可以使用Spring Boot提供的配置外部化能力*

## 模块列表如下：
* minicubes-core：提供基础的数据容器和聚合计算API。
* minicubes-cluster：提供分布式聚合计算能力。
* minicubes-lucene：TODO

## minicubes-core：
本模块提供内存型Cube的操作接口，设计目标就是：高性能
* 使用Java8 Stream来提高聚集方法性能，使用parallel模式。
* 使用[Bitmap Index](https://github.com/lemire/RoaringBitmap "compressed bitset")来增强部分聚集方法性能。
* 使用[DoubleDouble](http://tsusiatsoftware.net/dd/main.html "DoubleDouble")替换Java.math.BigDecimal来降低内存占用。

## minicubes-cluster：
本模块提供分布式计算能力，设计目标就是：高可用
* 使用[MySQL Streaming](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html "MySQL Streaming")来适应大结果集的加载。
* 使用[Hazelcast](https://github.com/hazelcast/hazelcast "Hazelcast")提供集群管理和分布式ExecutorService。
