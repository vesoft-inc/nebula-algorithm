# 欢迎使用 nebula-algorithm

nebula-algorithm 是一款基于 [GraphX](https://spark.apache.org/graphx/) 的 Spark 应用程序，提供了以下图计算算法：


 |           算法名          |中文说明|应用场景|
 |:------------------------:|:-----------:|:----:|
 |         PageRank         |  页面排序  | 网页排序、重点节点挖掘|
 |         Louvain          |  社区发现  | 社团挖掘、层次化聚类|
 |          KCore           |    K核    |社区发现、金融风控|
 |     LabelPropagation     |  标签传播  |资讯传播、广告推荐、社区发现|
 |    ConnectedComponent    |  联通分量  |社区发现、孤岛发现|
 |StronglyConnectedComponent| 强联通分量  |社区发现|
 |       ShortestPath       |  最短路径   |路径规划、网络规划|
 |       TriangleCount      | 三角形计数  |网络结构分析|
 |   BetweennessCentrality  | 介数中心性  |关键节点挖掘，节点影响力计算|
 |        DegreeStatic      |   度统计   |图结构分析|
 
使用 `nebula-algorithm`，可以通过提交 `Spark` 任务的形式使用完整的算法工具对 `Nebula Graph` 数据库中的数据执行图计算，也可以通过编程形式调用`lib`库下的算法针对DataFrame执行图计算。

## 如何获取
 1. 编译打包 Nebula-Algorithm
    ```
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-algorithm
    $ mvn clean package -Dgpg.skip -Dmaven.javadoc.skip=true -Dmaven.test.skip=true
    ```
    编译完成后，在 `nebula-algorithm/target` 目录下会生成 `nebula-algorithm-2.0.0.jar` 。

 2. 在 Maven 远程仓库下载
   https://repo1.maven.org/maven2/com/vesoft/nebula-algorithm/2.0.0/

# 使用 Nebula-Algorithm

   使用限制：Nebula-Algorithm 未自动对字符串id进行编码，因此执行图算法时，边的源点和目标点必须是整数（Nebula Space 的 vid_type可以是String类型，但数据必须是整数）。
   
* 使用方法1：直接提交 nebula-algorithm 算法包

   * 设置配置文件
   
    关于配置项的具体说明参考[示例配置](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-spark-utils/nebula-algorithm/src/main/resources/application.conf)

   * 提交算法任务

    ```
    ${SPARK_HOME}/bin/spark-submit --master <mode> --class com.vesoft.nebula.algorithm.Main nebula-algorithm-2.0.0.jar -p property_file
    ```
* 使用方法2：调用 nebula-algorithm 算法接口

   在`nebula-algorithm`的`lib`库中提供了10中常用图计算算法，可通过编程调用的形式调用算法。
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
   val prConfig = new PRConfig(5, 1.0)
   val louvainResult = PageRankAlgo.apply(spark, data, prConfig, false)
   ```
 
    其他算法的调用方法见[测试示例](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-spark-utils/nebula-algorithm/src/test/scala/com/vesoft/nebula/algorithm/lib) 。
    
    *注：执行算法的DataFrame默认第一列是源点，第二列是目标点，第三列是边权重。*

## 贡献

nebula-algorithm 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
