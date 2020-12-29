# 欢迎使用 Nebula Spark Connector 2.0

# 介绍

Nebula Spark Connector 2.0 仅支持 Nebula Graph 2.x。如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools)。

## 如何编译

Nebula Spark Connector 2.0 依赖 Nebula Java Client 2.0。

1. 编译打包 Nebula Java Client 2.0。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-java.git
    $ cd nebula-java
    $ mvn clean install -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true  
    ```

    打包结束后，在本地 Maven Repository 仓库中可以看到生成的 /com/vesoft/client/2.0.0-beta/client-2.0.0-beta.jar。

2. 编译打包 Nebula Spark Connector 2.0。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-utils/nebula-spark-connector/target/ 目录下看到 nebula-spark-connector-2.0.0.jar 文件。

## 使用说明

  将 DataFrame 作为点写入 Nebula Graph :
  ```
    val config =
      NebulaConnectionConfig
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

## 贡献

Nebula Spark Connector 2.0 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
