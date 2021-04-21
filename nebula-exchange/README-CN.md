# 欢迎使用 Nebula Exchange 2.0         
[English](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-exchange/README.md)

Nebula Exchange 2.0（简称为 Exchange 2.0）是一款 Apache Spark&trade; 应用，用于在分布式环境中将集群中的数据批量迁移到 Nebula Graph 中，能支持多种不同格式的批式数据和流式数据的迁移。

Exchange 2.0 仅支持 Nebula Graph 2.x。

如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Exchange v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/exchange) ，或参考 Exchange 1.0 的使用文档[《Nebula Exchange 用户手册》](https://docs.nebula-graph.com.cn/nebula-exchange/about-exchange/ex-ug-what-is-exchange/ "点击前往 Nebula Graph 网站")。

## 如何获取

1. 编译打包 Exchange 2.0。

    ```bash
    $ git clone -b v2.0.0 https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-exchange
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-utils/nebula-exchange/target/ 目录下看到 nebula-exchange-2.0.0.jar 文件。
2. 在 Maven 远程仓库下载
    
    https://repo1.maven.org/maven2/com/vesoft/nebula-exchange/2.0.0/
## 使用说明

特性 & 注意事项：

*1. Nebula Graph 2.0 支持 String 类型和 Integer 类型的点 id 。*

*2. Exchange 2.0 新增 null、Date、DateTime、Time 类型数据的导入（ DateTime 是 UTC 时区，非 Local time）。*

*3. Exchange 2.0 支持 Hive on Spark 以外的 Hive 数据源，需在配置文件中配置 Hive 源，具体配置示例参考 [server_application.conf](https://github.com/vesoft-inc/nebula-spark-utils/blob/main/nebula-exchange/src/main/resources/server_application.conf) 中 Hive 的配置。*

*4. Exchange 2.0 将导入失败的 INSERT 语句进行落盘，存于配置文件的 error/output 路径中。*

*5. 配置文件参考 [application.conf](https://github.com/vesoft-inc/nebula-spark-utils/tree/main/nebula-exchange/src/main/resources)。*

*6. Exchange 2.0 的导入命令：*
```
$SPARK_HOME/bin/spark-submit --class com.vesoft.nebula.exchange.Exchange --master local nebula-exchange-2.0.0.jar -c /path/to/application.conf
```
## 贡献

Nebula Exchange 2.0 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
