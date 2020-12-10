# 欢迎使用 Nebula Exchange 2.0

Nebula Exchange（简称为 Exchange），是一款 Apache Spark&trade; 应用，用于在分布式环境中将集群中的数据批量迁移到 Nebula Graph 中，能支持多种不同格式的批式数据和流式数据的迁移。

## 如何编译
Nebula Exchange 2.0 依赖 Nebula java client。

1. 编译打包 Nebula Java Client
    ```
    $ git clone https://github.com/vesoft-inc/nebula-java.git
    $ cd nebula-java
    $ mvn clean compile package install -Dtest.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true  
    ```
    打包结束后在本地 maven repository 中会生成 /com/vesoft/client/2.0.0-beta/client-2.0.0-beta.jar。

2. 编译打包 Nebula Exchange 2.0
    ```
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-exchange
    $ mvn clean compile package install  -Dtest.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```
    编译打包完成后，在 nebula-spark-utils/nebula-exchange/target/ 目录下生成 nebula-exchange-2.0.0.jar。

## 使用说明

关于 Nebula Exchange 2.0 的详细使用说明，请参考1.0的使用文档[《Nebula Exchange 用户手册》](https://docs.nebula-graph.com.cn/nebula-exchange/about-exchange/ex-ug-what-is-exchange/ "点击前往 Nebula Graph 网站")。

注意事项：

*1.  Nebula Graph 2.0暂时只支持String类型的vid，请勿配置点id或者边的src_id、dst_id的policy*

*2.  Nebula Exchange 2.0 支持Date、DateTime、Time类型数据的导入*

*3.  配置文件参考 [application.conf](https://github.com/vesoft-inc/nebula-spark-utils/tree/main/nebula-exchange/src/main/resources)*
## 贡献

Nebula Exchange 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
