# 欢迎使用 Nebula Algorithm

nebula-algorithm 是一款基于 [GraphX](https://spark.apache.org/graphx/) 的 Spark 应用程序，提供了以下图计算算法：


 |           算法名          |中文说明|应用场景|
 |:------------------------:|:-----------:|:----:|
 |         PageRank         |  页面排序  | 网页排序、重点节点挖掘|
 |         Louvain          |  社区发现  | 社团挖掘、层次化聚类|
 |          KCore           |    K核    |社区发现、金融风控|
 |     LabelPropagation     |  标签传播  |资讯传播、广告推荐、社区发现|
 |          Hanp            |  标签传播进阶版|社区发现、推荐|
 |    ConnectedComponent    |  联通分量  |社区发现、孤岛发现|
 |StronglyConnectedComponent| 强联通分量  |社区发现|
 |       ShortestPath       |  最短路径   |路径规划、网络规划|
 |       TriangleCount      | 三角形计数  |网络结构分析|
 |     GraphTriangleCount   |全图三角形计数|网络紧密性分析|
 |   BetweennessCentrality  | 介数中心性  |关键节点挖掘、节点影响力计算|
 |        Closeness         | 接近中心性  |关键节点挖掘、节点影响力计算|
 |        DegreeStatic      |   度统计   |图结构分析|
 |   ClusteringCoefficient  |  聚集系数  |推荐、电信诈骗分析|
 |         Jaccard          |杰卡德相似度计算|相似度计算、推荐|
 |          BFS             |广度优先遍历 |层序遍历、最短路径规划|
 |         Node2Vec         |    -      |图分类|
 
使用 `nebula-algorithm`，可以通过提交 `Spark` 任务的形式使用完整的算法工具对 `Nebula Graph` 数据库中的数据执行图计算，也可以通过编程形式调用`lib`库下的算法针对DataFrame执行图计算。

## 如何获取
 1. 编译打包 Nebula Algorithm
    ```
    $ git clone https://github.com/vesoft-inc/nebula-algorithm.git
    $ cd nebula-algorithm
    $ mvn clean package -Dgpg.skip -Dmaven.javadoc.skip=true -Dmaven.test.skip=true
    ```
    编译完成后，在 `nebula-algorithm/target` 目录下会生成 `nebula-algorithm-3.0-SNAPSHOT.jar` 。

 2. 在 Maven 远程仓库下载
   https://repo1.maven.org/maven2/com/vesoft/nebula-algorithm/

# 使用 Nebula Algorithm
   
* 使用方法1：直接提交 nebula-algorithm 算法包

   * 设置配置文件
   
    关于配置项的具体说明参考[示例配置](https://github.com/vesoft-inc/nebula-algorithm/blob/master/nebula-algorithm/src/main/resources/application.conf)

   * 提交算法任务

    ```
    ${SPARK_HOME}/bin/spark-submit --master <mode> --class com.vesoft.nebula.algorithm.Main nebula-algorithm-3.0-SNAPSHOT.jar -p application.conf
    ```
    * 使用限制
    
    Nebula Algorithm 算法包未自动对字符串 id 进行编码，因此采用第一种方式执行图算法时，边的源点和目标点必须是整数（Nebula Space 的 vid_type 可以是 String 类型，但数据必须是整数）。
* 使用方法2：调用 nebula-algorithm 算法接口

   在 `nebula-algorithm` 的 `lib` 库中提供了10+种常用图计算算法，可通过编程调用的形式调用算法。
   * 在pom.xml中添加依赖
   ```
    <dependency>
         <groupId>com.vesoft</groupId>
         <artifactId>nebula-algorithm</artifactId>
         <version>2.0.0</version>
    </dependency>
   ```
   * 定义算法参数调用算法（以`PageRank`为例）
   ```
   import com.vesoft.nebula.algorithm.config.{Configs, PRConfig, SparkConfig}
   import org.apache.spark.sql.{DataFrame, SparkSession}

   val spark = SparkSession.builder().master("local").getOrCreate()
   val data  = spark.read.option("header", true).csv("src/test/resources/edge.csv")
   val prConfig = new PRConfig(5, 1.0)
   val prResult = PageRankAlgo.apply(spark, data, prConfig, false)
   ```
   * 如果你的节点 id 是 String 类型，可以参考 PageRank 的 [Example](https://github.com/vesoft-inc/nebula-algorithm/blob/master/example/src/main/scala/com/vesoft/nebula/algorithm/PageRankExample.scala) 。 
   该 Example 进行了 id 转换，将 String 类型 id 编码为 Long 类型的 id ， 并在算法结果中将 Long 类型 id 解码为原始的 String 类型 id 。
   
    其他算法的调用方法见[测试示例](https://github.com/vesoft-inc/nebula-algorithm/tree/master/nebula-algorithm/src/test/scala/com/vesoft/nebula/algorithm/lib) 。
    
    > 注：执行算法的 DataFrame 默认第一列是源点，第二列是目标点，第三列是边权重。

## Nebula 中属性配置
    如果你想将算法结果写入到 Nebula，请确保 Nebula 的 tag 中有对应算法结果名称的属性。各项算法对应的属性名如下所示：

    |        Algorithm         |     property name       |property type|
    |:------------------------:|:-----------------------:|:-----------:|
    |         pagerank         |         pagerank        |double/string|
    |          louvain         |          louvain        | int/string  |
    |          kcore           |           kcore         | int/string  |
    |     labelpropagation     |           lpa           | int/string  |
    |   connectedcomponent     |            cc           | int/string  |
    |stronglyconnectedcomponent|            scc          | int/string  |
    |         betweenness      |         betweenness     |double/string|
    |        shortestpath      |        shortestpath     |   string    |
    |        degreestatic      |degree,inDegree,outDegree| int/string  |
    |        trianglecount     |       trianglecount     | int/string  |
    |  clusteringcoefficient   |    clustercoefficient   |double/string|
    |         closeness        |         closeness       |double/string|
    |            hanp          |            hanp         | int/string  |
    |            bfs           |            bfs          |    string   |
    |         jaccard          |          jaccard        |    string   |
    |        node2vec          |          node2vec       |    string   |
    
## 版本匹配

| Nebula Algorithm Version | Nebula Version |
|:------------------------:|:--------------:|
|       2.0.0              |  2.0.0, 2.0.1  |
|       2.1.0              |  2.0.0, 2.0.1  |
|       2.5.0              |  2.5.0, 2.5.1  |
|       2.6.0              |  2.6.0, 2.6.1  |
|       2.6.2              |  2.6.0, 2.6.1  |
|     3.0-SNAPSHOT         |     nightly    |

## 贡献

Nebula Algorithm 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
