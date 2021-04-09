# 使用限制

本文描述 Exchange v1.x 的一些使用限制。

## Nebula Graph 版本

Exchange v1.x 仅支持 Nebula Graph v1.x。如果您正在使用 Nebula Graph v2.x，请使用 [Nebula Exchange v2.x](https://github.com/vesoft-inc/nebula-spark-utils "点击前往 GitHub")。

## 使用环境

Exchange v1.x 支持以下操作系统：

- CentOS 7
- macOS

> **说明**：仅 Linux 系统支持导入 SST 文件。

## 软件依赖

为保证 Exchange v1.x 正常工作，确认您的机器上已经安装以下软件：

- Apache Spark：2.3.0 及以上版本
- Java：1.8
- Scala：2.10.7、2.11.12、2.12.10

在以下使用场景，还需要部署 Hadoop Distributed File System (HDFS)：

- 以客户端形式迁移 HDFS 上的数据
- 以 SST 文件格式迁移数据
