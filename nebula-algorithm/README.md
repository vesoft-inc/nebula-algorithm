# Welcome to Nebula-Algorithm

<p align="center">
  <br>English | <a href="README-CN.md">中文</a>
</p>

nebula-algorithm is a Spark Application based on [GraphX](https://spark.apache.org/graphx/) with below Algorithm provided for now:


|          Name          |Use Case|
|:------------------------:|:---------------:|
|         PageRank         | 网页排序、重点节点挖掘|
|         Louvain          | 社团挖掘、层次化聚类|
|          KCore             |社区发现、金融风控|
|     LabelPropagation     |资讯传播、广告推荐、社区发现|
|    ConnectedComponent    |社区发现、孤岛发现|
|StronglyConnectedComponent|社区发现|
|       ShortestPath        |路径规划、网络规划|
|       TriangleCount      |网络结构分析|
|   BetweennessCentrality  |关键节点挖掘，节点影响力计算|

You could either leverage comprehensive Nebula-Algorithm tooling in way to submit its `Spark Application` or invoke its `lib` in a programming way to apply Graph Compute towards the `DataFrame`.

## Get Nebula-Algorithm
 1. Build Nebula-Algorithm
    ```
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-algorithm
    $ mvn clean package -Dgpg.skip -Dmaven.javadoc.skip=true -Dmaven.test.skip=true
    ```
    After the above buiding process the target file  `nebula-algorithm-2.0.0.jar` will be placed under `nebula-algorithm/target` .

 2. Download from Maven repo
      
      Alternatively, it could be downlaoded from below Maven repo:
      
      https://repo1.maven.org/maven2/com/vesoft/nebula-algorithm/2.0.0/

## Use Nebula-Algorithm

Limitation: Due to Nebula-Algorithm will not encode string id, thus during the algorithm execution, the source and target of edges must be in Type Int (The `vid_type` in Nebula Space could be String, while data must be in Type Int).

* Option 1: Submit nebula-algorithm package

   * Configuration
   
   Refer to [configuration example](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-algorithm/src/main/resources/application.conf)

   * Submit Spark Applicaiton

    ```
    ${SPARK_HOME}/bin/spark-submit --master <mode> --class com.vesoft.nebula.algorithm.Main nebula-algorithm-2.0.0.jar -p application.conf
    ```
   
* Option2: Call nebula-algorithm interface

   Now there are 10 algorithms provided in `lib` from `nebula-algorithm`, which could be invoked in a programming fashion as below:
   
   * Add depedencies from `pom.xml`
   ```
    <dependency>
         <groupId>com.vesoft</groupId>
         <artifactId>nebula-algorithm</artifactId>
         <version>2.0.0</version>
    </dependency>
   ```
   * Pass arguments of the algorithm, below is an example for `PageRank`.
   ```
   val prConfig = new PRConfig(5, 1.0)
   val louvainResult = PageRankAlgo.apply(spark, data, prConfig, false)
   ```
   
    For other algorithms, please refer to [test cases](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-algorithm/src/test/scala/com/vesoft/nebula/algorithm/lib).
   
   *Note: The first column of DataFrame in the application are the source vertices, the second are the target vertices and the third are edges' weight.*

## Contribute

nebula-algorithm is an Open-Source project, you are more than welcomed to contribute in below ways:

- Discuss in the community via [the forum](https://discuss.nebula-graph.io/) or raise issues here.
- Compose or improve our documents.
- Pull Request to help improve the code itself here.