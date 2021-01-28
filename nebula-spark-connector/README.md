# Nebula Spark Connector 2.0
[中文版](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-spark-connector/README_CN.md)

## Introduction

Nebula Spark Connector 2.0 only supports Nebula Graph 2.x. If you are using Nebula Graph v1.x, please use [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-spark) .

## How to Compile

Nebula Spark Connector 2.0 depends on the latest Nebula Java Client 2.0.

1. Install Nebula Java Client 2.0.

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-java.git
    $ cd nebula-java
    $ mvn clean install -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true  
    ```

    After the installation, you can see the newly generated /com/vesoft/client/2.0.0-rc1/client-2.0.0-rc1.jar in your local Maven repository.

2. Package Nebula Spark Connector 2.0.

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    After the packaging, you can see the newly generated nebula-spark-connector-2.0.0.jar under the nebula-spark-utils/nebula-spark-connector/target/ directory.

## New Features (Compared to Nebula Spark Connector 1.0)
* Supports more connection configurations, such as timeout, connectionRetry, and executionRetry.
* Supports more data configurations, such as whether vertexId can be written as vertex's property, whether srcId, dstId and rank can be written as edge's properties.
* Spark Reader Supports non-property, all-property, and specific-properties read.
* Spark Reader Supports reading data from Nebula Graph to Graphx as VertexRD and EdgeRDD, it also supports String type vertexId.
* Nebula Spark Connector 2.0 uniformly uses SparkSQL's DataSourceV2 for data source expansion.

## How to Use

  Write DataFrame into Nebula Graph as Vertices:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:45500")
      .withGraphAddress("127.0.0.1:3699")
      .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
  Read vertices from Nebula Graph: 
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:45500")
      .withConenctionRetry(2)
      .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
  ```

  Read vertices and edges from Nebula Graph to construct Graphx's graph:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:45500")
      .build()
    val nebulaReadVertexConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val nebulaReadEdgeConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("knows1")
      .withNoColumn(false)
      .withReturnCols(List("timep"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToGraphx()
    val edgeRDD = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToGraphx()
    val graph = Graph(vertexRDD, edgeRDD)
  ```
  After getting Graphx's Graph, you can develop graph algorithms in Graphx like [Nebula-Spark-Algorithm](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-algorithm).

For more information on usage, please refer to [Example](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/example/src/main/scala/com/vesoft/nebula/examples/connector).

## How to Contribute

Nebula Spark Connector 2.0 is a completely opensource project, opensource enthusiasts are welcome to participate in the following ways:

- Go to [Nebula Graph Forum](https://discuss.nebula-graph.com.cn/ "go to“Nebula Graph Forum") to discuss with other users. You can raise your own questions, help others' problems, share your thoughts.
- Write or improve documents.
- Submit code to add new features or fix bugs.
