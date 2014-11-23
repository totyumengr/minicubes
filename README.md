MiniCubes
=========

MiniCubes是一个轻量级、高性能、分布式、内存型OLAP计算引擎，提供**分布式计算函数**有：
* sum：指定指标的SUM聚集计算。
* groupby-sum；指定指标在某一维度上的GROUPBY SUM聚合计算。
* count：指定指标的COUNT聚集计算。
* groupby-count；指定指标在某一维度上的GROUPBY COUNT聚合计算。
* groupby-distinct：指定指标/维度在某一维度上的DISTINCT计算。
* groupby-distinct-count：指定指标/维度在某一维度上的DISTINCT-COUNT计算。

MiniCubes作为[百度BI平台](https://github.com/Baidu-ecom/bi-platform "百度BI平台")的原形项目，会实验性使用一些新技术：
* Java8 Nashorn：使用Nashorn引擎在索引构建阶段加入自定义扩展功能，可提供原始数据中没有的维度列，增强查询能力。
* JavaFX 8：使用JavaFX构建客户端软件，并发布运行在Andriod、iOS的环境中，增加移动BI特性。

## 设计原则：
* MiniCubes追求极致的fat-table设计，支持在数据导入阶段增加自定义维度索引列。
* MiniCubes倾向于构建“自包含”的微服务，**只依赖JVM**即可运行，简单极致的集群化部署流程。
* MiniCubes设计上追求极致简单，代码量只有**2k**行左右，未来也会尽力保持她小而美。

## 使用&依赖：
* Maven依赖（已发布到Maven Central Repository）：
```
<dependency>
    <groupId>com.github.totyumengr</groupId>
    <artifactId>minicubes-core</artifactId>
    <version>${minicubes.version}</version>
</dependency>
<dependency>
    <groupId>com.github.totyumengr</groupId>
    <artifactId>minicubes-cluster</artifactId>
    <version>${minicubes.version}</version>
</dependency>
```
* 可执行Fat Jar：
```
cd <dir of minicubes>
mvn -Pfat clean install -DskipTests=true
java -server -jar minicubes-cluster-VERSIONS.jar
http://localhost:PORT/status
```

* 多次运行，可在单机上构建JVM集群：
```
java -server -jar minicubes-cluster-VERSIONS.jar
```
*使用Spring Boot提供的配置外部化能力，来构建企业级集群*

## 数据：
* Java8 Stream parallel平均会快于sequential，但parallel模式稳定度不如sequential。
    1. 循环执行几次sum聚集计算，parallel模式的第一次通常较慢（和sequential模式持平）；
    2. vm上800w数据全量sum，parallel模式（约190ms）比sequential模式（约850ms）快4倍左右；
    3. 物理机上2000w数据全量sum，parallel模式（约230ms）比sequential模式（约2秒）快9倍左右；
* 8G内存中大约可以存放2.5kw条记录（5个维度和4个指标），这些原始数据大小应该在1.5G。
* 3台物理机+1台虚拟机搭建集群上载入大概1亿条数据：
    1. 单指标全量sum耗时平均400ms左右；
    2. 单指标全量groupby-sum耗时平均600ms左右，结果7000条左右；
    3. 单指标全量groupby-distinct耗时平均800ms左右，结果7000条左右；
    4. 单指标全量groupby-distinctcount耗时平均800ms左右，结果7000条左右；
    5. 虚拟机性能是物理机的1/3~1/2,所以请保持虚拟机载入数据量为物理机1/3~1/2；
* 我并没有发现Java8 Stream的[Concurrent Reduction](http://docs.oracle.com/javase/tutorial/collections/streams/parallelism.html#concurrent_reduction "Concurrent Reduction")会更快。
* 使用[Bitmap Index](https://github.com/lemire/RoaringBitmap "compressed bitset")对21147413条记录的5个维度（共340386维值）进行索引，占用内存180M。
* 2000w数据量在单机MySQL和Java8 Stream上性能对比：

    |场景|MySQL|Java8|
    |:---------------|---------------:|---------------:|
    |单指标全量sum|8850ms|210ms|
    |单指标全量groupby-sum|24860ms|380ms|
    |单指标全量groupby-distinctcount|26050ms|430ms|
    |过滤1000个维值&&200个维值，单指标groupby-sum|34110ms|380ms|
    |过滤1000个维值，单指标sum|12560ms|240ms|
    |过滤40个维值&&30个维值，单指标groupby-sum|300ms|240ms|
    |过滤6991个维值 && 201个维值，单指标sum操作|TODO|600ms|


**以上基于CPU12核/内存128G的物理机和CPU8核/16G内存的虚拟机搭建集群测试得出**

## 模块列表如下：
#### minicubes-core：
本模块提供内存型Cube的操作接口，设计目标是：高性能
* 使用[Java8 Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html "Java8 Stream")来提高聚集方法性能，使用parallel模式。
* 使用[Bitmap Index](https://github.com/lemire/RoaringBitmap "compressed bitset")来增强部分聚集方法性能。
* 使用[DoubleDouble](http://tsusiatsoftware.net/dd/main.html "DoubleDouble")替换Java.math.BigDecimal来降低内存占用。

*注意这里有个坑：new DoubleDouble("1926")的结果是0.1926，需要改为new DoubleDouble("1926.00000000")*

#### minicubes-cluster：
本模块提供分布式计算能力，设计目标是：高可用
* 使用[MySQL Streaming](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html "MySQL Streaming")来适应大结果集的加载。
* 使用[Hazelcast](https://github.com/hazelcast/hazelcast "Hazelcast")提供集群管理和分布式ExecutorService。
* 使用maven-surefire插件的[Fork Mode](http://maven.apache.org/surefire/maven-surefire-plugin/examples/fork-options-and-parallel-execution.html "Fork Mode")，来做集群部署集成测试，保证代码质量。

#### minicubes-fxclient（开发中尚未正式发布）：
本模块是客户端程序，设计目标是：多端使用
* 使用[JavaFX 8](https://docs.oracle.com/javase/8/javase-clienttechnologies.htm "JavaFX 8")来开发本地客户端，并可在Android甚至iOS上运行。