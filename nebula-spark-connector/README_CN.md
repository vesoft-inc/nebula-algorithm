# 欢迎使用 Nebula Spark Connector 2.0
[English](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-spark-connector/README.md)
## 介绍

Nebula Spark Connector 2.0 仅支持 Nebula Graph 2.x。如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools)。

## 如何编译

1. 编译打包 Nebula Spark Connector 2.0。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-utils/nebula-spark-connector/target/ 目录下看到 nebula-spark-connector-2.0.0.jar 文件。

## 特性

* 提供了更多连接配置项，如超时时间、连接重试次数、执行重试次数
* 提供了更多数据配置项，如写入数据时是否将 vertexId 同时作为属性写入、是否将 srcId、dstId、rank 等同时作为属性写入
* Spark Reader 支持无属性读取，支持全属性读取
* Spark Reader 支持将 Nebula Graph 数据读取成 Graphx 的 VertexRD 和 EdgeRDD，支持非 Long 型 vertexId
* Nebula Spark Connector 2.0 统一了 SparkSQL 的扩展数据源，统一采用 DataSourceV2 进行 Nebula Graph 数据扩展

## 使用说明

  将 DataFrame 作为点写入 Nebula Graph :
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
  读取 Nebula Graph 的点数据: 
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withConenctionRetry(2)
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
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
  ```

  读取 Nebula Graph 的点边数据构造 Graphx 的图：
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withConenctionRetry(2)
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
  得到 Graphx 的 Graph 之后，可以根据 [Nebula-Spark-Algorithm](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-algorithm) 的示例在 Graphx 框架中进行算法开发。

更多使用示例请参考 [Example](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/example/src/main/scala/com/vesoft/nebula/examples/connector) 。

## 贡献

Nebula Spark Connector 2.0 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
