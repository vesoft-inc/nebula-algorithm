# Welcome to Nebula Algorithm

<p align="center">
  <br>English | <a href="README-CN.md">中文</a>
</p>

nebula-algorithm is a Spark Application based on [GraphX](https://spark.apache.org/graphx/) with the following Algorithm provided for now:


|          Name          |Use Case|
|:------------------------:|:---------------:|
|         PageRank         | page ranking, important node digging|
|         Louvain          | community digging, hierarchical clustering|
|          KCore           | community detection, financial risk control|
|     LabelPropagation     | community detection, consultation propagation, advertising recommendation|
|          Hanp            | community detection, consultation propagation|
|    ConnectedComponent    | community detection, isolated island detection|
|StronglyConnectedComponent| community detection|
|       ShortestPath       | path plan, network plan|
|       TriangleCount      | network structure analysis|
|    GraphTriangleCount    | network structure and tightness analysis|
|   BetweennessCentrality  | important node digging, node influence calculation|
|   ClosenessCentrality    | important node digging, node influence calculation|
|        DegreeStatic      | graph structure analysis|
|   ClusteringCoefficient  | recommended, telecom fraud analysis|
|       Jaccard            | similarity calculation, recommendation|
|        BFS               | sequence traversal, Shortest path plan|
|     Node2Vec             | graph machine learning, recommendation|


You could submit the entire spark application or invoke algorithms in `lib` library to apply graph algorithms for DataFrame.

## Get Nebula Algorithm
 1. Build Nebula Algorithm
    ```
    $ git clone https://github.com/vesoft-inc/nebula-algorithm.git
    $ cd nebula-algorithm
    $ mvn clean package -Dgpg.skip -Dmaven.javadoc.skip=true -Dmaven.test.skip=true
    ```
    After the above buiding process, the target file  `nebula-algorithm-3.0-SNAPSHOT.jar` will be placed under `nebula-algorithm/target`.

 2. Download from Maven repo
      
      Alternatively, it could be downloaded from the following Maven repo:
      
      https://repo1.maven.org/maven2/com/vesoft/nebula-algorithm/

## Use Nebula Algorithm

* Option 1: Submit nebula-algorithm package

   * Configuration
   
   Refer to the [configuration example](https://github.com/vesoft-inc/nebula-algorithm/blob/master/nebula-algorithm/src/main/resources/application.conf).

   * Submit Spark Application

    ```
    ${SPARK_HOME}/bin/spark-submit --master <mode> --class com.vesoft.nebula.algorithm.Main nebula-algorithm-3.0—SNAPSHOT.jar -p application.conf
    ```
   
   * Limitation
    
    Due to Nebula Algorithm jar does not encode string id, thus during the algorithm execution, the source and target of edges must be in Type Int (The `vid_type` in Nebula Space could be String, while data must be in Type Int).

* Option2: Call nebula-algorithm interface

   Now there are 10+ algorithms provided in `lib` from `nebula-algorithm`, which could be invoked in a programming fashion as below:
   
   * Add dependencies in `pom.xml`.
   ```
    <dependency>
         <groupId>com.vesoft</groupId>
         <artifactId>nebula-algorithm</artifactId>
         <version>2.0.0</version>
    </dependency>
   ```
   * Instantiate algorithm's config, below is an example for `PageRank`.
   ```
   import com.vesoft.nebula.algorithm.config.{Configs, PRConfig, SparkConfig}
   import org.apache.spark.sql.{DataFrame, SparkSession}

   val spark = SparkSession.builder().master("local").getOrCreate()
   val data  = spark.read.option("header", true).csv("src/test/resources/edge.csv")
   val prConfig = new PRConfig(5, 1.0)
   val prResult = PageRankAlgo.apply(spark, data, prConfig, false)
   ```
   
   If your vertex ids are Strings, see [Pagerank Example](https://github.com/vesoft-inc/nebula-algorithm/blob/master/example/src/main/scala/com/vesoft/nebula/algorithm/PageRankExample.scala) for how to encoding and decoding them.
    
    For examples of other algorithms, see [examples](https://github.com/vesoft-inc/nebula-algorithm/tree/master/example/src/main/scala/com/vesoft/nebula/algorithm)
   > Note: The first column of DataFrame in the application represents the source vertices, the second represents the target vertices and the third represents edges' weight.

## Nebula config
    If you want to write the algorithm result into Nebula, make sure there is corresponding property name in your tag.
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
    
## Version match

| Nebula Algorithm Version | Nebula Version |
|:------------------------:|:--------------:|
|       2.0.0              |  2.0.0, 2.0.1  |
|       2.1.0              |  2.0.0, 2.0.1  |
|       2.5.0              |  2.5.0, 2.5.1  |
|       2.6.0              |  2.6.0, 2.6.1  |
|       2.6.1              |  2.6.0, 2.6.1  |
|       3.0-SNAPSHOT       |     nightly    |

## Contribute

Nebula Algorithm is open source, you are more than welcomed to contribute in the following ways:

- Discuss in the community via [the forum](https://discuss.nebula-graph.io/) or raise issues here.
- Compose or improve our documents.
- Pull Request to help improve the code itself here.
