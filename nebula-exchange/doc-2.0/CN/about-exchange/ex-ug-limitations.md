# 使用限制

本文描述Exchange 2.0的一些使用限制。

## Nebula Graph版本

Exchange client版本（即JAR包版本）和Nebula Graph的版本对应关系如下。
|Exchange client版本|Nebula Graph版本|
|:---|:---|
|2.0.0|2.0.0|
|2.0.0|2.0.1|

JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

如果您正在使用Nebula Graph 1.x，请使用[Nebula Exchange 1.x](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools "Click to go to GitHub")。

## 使用环境

Exchange 2.0 支持以下操作系统：

- CentOS 7
- macOS

> **说明**：仅Linux系统支持导入SST文件。

## 软件依赖

为保证Exchange正常工作，请确认您的机器上已经安装如下软件：

- Apache Spark：2.3.0及以上版本

- Java：1.8

- Scala：2.10.7、2.11.12或2.12.10

在以下使用场景，还需要部署Hadoop Distributed File System (HDFS)：

- 迁移HDFS的数据
- 生成SST文件
