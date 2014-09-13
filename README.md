MiniCubes
=========

MiniCubes是一个高性能、分布式、内存型OLAP计算引擎。设计上利用Java8 Stream的高性能计算特性来支撑单JVM节点上的聚集计算，基于Hazelcast#Distributed ExecutorService来做分布式计算。

## 本地开发：
我们采用Spring Boot来构建“自包含”的应用，JDK1.8，Maven3：
* 1. minicubes-core目录下执行：mvn clean install -DskipTests=true
* 2. minicubes-cluster目录下执行：mvn clean install -DskipTests=true
* 3. Maven仓库目录下：com/github/totyumengr/minicubes-cluster/目录下，找到jar包
* 4. 直接执行：java -server -jar minicubes-cluster-VERSIONS.jar
* 5. 访问：http://localhost:PORT/[status,reassign,sum]

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

## 数据：
* 8G内存中可以存放26788234条记录（5个维度和4个指标），这些原始数据大小应该在1.5G。
