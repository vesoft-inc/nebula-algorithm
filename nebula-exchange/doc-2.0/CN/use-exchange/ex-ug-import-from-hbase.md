# 导入HBase数据

本文以一个示例说明如何使用Exchange将存储在HBase上的数据导入Nebula Graph。

## 数据集

本文以[basketballplayer数据集](https://docs-cdn.nebula-graph.com.cn/dataset/dataset.zip)为例。

在本示例中，该数据集已经存入HBase中，以`player`、`team`、`follow`和`serve`四个表存储了所有点和边的信息。以下为各个表的部分数据。

```sql
hbase(main):002:0> scan "player"
ROW                                COLUMN+CELL
 player100                         column=cf:age, timestamp=1618881347530, value=42
 player100                         column=cf:name, timestamp=1618881354604, value=Tim Duncan
 player101                         column=cf:age, timestamp=1618881369124, value=36
 player101                         column=cf:name, timestamp=1618881379102, value=Tony Parker
 player102                         column=cf:age, timestamp=1618881386987, value=33
 player102                         column=cf:name, timestamp=1618881393370, value=LaMarcus Aldridge
 player103                         column=cf:age, timestamp=1618881402002, value=32
 player103                         column=cf:name, timestamp=1618881407882, value=Rudy Gay
 ...

hbase(main):003:0> scan "team"
ROW                                COLUMN+CELL
 team200                           column=cf:name, timestamp=1618881445563, value=Warriors
 team201                           column=cf:name, timestamp=1618881453636, value=Nuggets
 ...

hbase(main):004:0> scan "follow"
ROW                                COLUMN+CELL
 player100                         column=cf:degree, timestamp=1618881804853, value=95
 player100                         column=cf:dst_player, timestamp=1618881791522, value=player101
 player101                         column=cf:degree, timestamp=1618881824685, value=90
 player101                         column=cf:dst_player, timestamp=1618881816042, value=player102
 ...

hbase(main):005:0> scan "serve"
ROW                                COLUMN+CELL
 player100                         column=cf:end_year, timestamp=1618881899333, value=2016
 player100                         column=cf:start_year, timestamp=1618881890117, value=1997
 player100                         column=cf:teamid, timestamp=1618881875739, value=team204
 ...
```

## 环境配置

本文示例在MacOS下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.4.7，单机版

- Hadoop：2.9.2，伪分布式部署

- HBase：2.2.7

- Nebula Graph：2.0.0。使用[Docker Compose部署](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)。

## 前提条件

开始导入数据之前，您需要确认以下信息：

- 已经[安装部署Nebula Graph](https://docs.nebula-graph.com.cn/2.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/2.install-nebula-graph-by-rpm-or-deb/)并获取如下信息：

  - Graph服务和Meta服务的的IP地址和端口。

  - 拥有Nebula Graph写权限的用户名和密码。

- 已经编译Exchange。详情请参见[编译Exchange](../ex-ug-compile.md)。本示例中使用Exchange 2.0。

- 已经安装Spark。

- 了解Nebula Graph中创建Schema的信息，包括标签和边类型的名称、属性等。

- 已经安装并开启Hadoop服务。

## 操作步骤

### 步骤 1：在Nebula Graph中创建Schema

分析数据，按以下步骤在Nebula Graph中创建Schema：

1. 确认Schema要素。Nebula Graph中的Schema要素如下表所示。

    | 要素  | 名称 | 属性 |
    | :--- | :--- | :--- |
    | 标签（Tag） | `player` | `name string, age int` |
    | 标签（Tag） | `team` | `name string` |
    | 边类型（Edge Type） | `follow` | `degree int` |
    | 边类型（Edge Type） | `serve` | `start_year int, end_year int` |

2. 在Nebula Graph中创建一个图空间**basketballplayer**，并创建一个Schema，如下所示。

    ```ngql
    ## 创建图空间
    nebula> CREATE SPACE basketballplayer \
            (partition_num = 10, \
            replica_factor = 1, \
            vid_type = FIXED_STRING(30));
    
    ## 选择图空间basketballplayer
    nebula> USE basketballplayer;
    
    ## 创建标签player
    nebula> CREATE TAG player(name string, age int);
    
    ## 创建标签team
    nebula> CREATE TAG team(name string);
    
    ## 创建边类型follow
    nebula> CREATE EDGE follow(degree int);

    ## 创建边类型serve
    nebula> CREATE EDGE serve(start_year int, end_year int);
    ```

更多信息，请参见[快速开始](https://docs.nebula-graph.com.cn/2.0/2.quick-start/1.quick-start-workflow/)。

### 步骤 2：修改配置文件

编译Exchange后，复制`target/classes/application.conf`文件设置HBase数据源相关的配置。在本示例中，复制的文件名为`hbase_application.conf`。各个配置项的详细说明请参见[配置说明](../parameter-reference/ex-ug-parameter.md)。

```conf
{
  # Spark相关配置
  spark: {
    app: {
      name: Nebula Exchange 2.0
    }
    driver: {
      cores: 1
      maxResultSize: 1G
    }
    cores {
      max: 16
    }
  }


  # Nebula Graph相关配置
  nebula: {
    address:{
      # 以下为Nebula Graph的Graph服务和Meta服务所在机器的IP地址及端口。
      # 如果有多个地址，格式为 "ip1:port","ip2:port","ip3:port"。
      # 不同地址之间以英文逗号 (,) 隔开。
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }
    # 填写的账号必须拥有Nebula Graph相应图空间的写数据权限。
    user: root
    pswd: nebula
    # 填写Nebula Graph中需要写入数据的图空间名称。
    space: basketballplayer
    connection {
      timeout: 3000
      retry: 3
    }
    execution {
      retry: 3
    }
    error: {
      max: 32
      output: /tmp/errors
    }
    rate: {
      limit: 1024
      timeout: 1000
    }
  }
  # 处理点
  tags: [
    # 设置标签player相关信息。
    # 如果需要将rowkey设置为数据源，请填写“rowkey”,列族内的列请填写实际列名。
    {
      # Nebula Graph中对应的标签名称。
      name: player
      type: {
        # 指定数据源文件格式，设置为HBase。
        source: hbase
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }
      host:192.168.*.*
      port:2181
      table:"player"
      columnFamily:"cf"

      # 在fields里指定player表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。
      fields: [age,name]
      nebula.fields: [age,name]

      # 指定表中某一列数据为Nebula Graph中点VID的来源。
      # 例如rowkey作为VID的来源，请填写“rowkey”。
      vertex:{
          field:rowkey
      }


      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }
    # 设置标签team相关信息。
    {
      name: team
      type: {
        source: hbase
        sink: client
      }
      host:192.168.*.*
      port:2181
      table:"team"
      columnFamily:"cf"
      fields: [name]
      nebula.fields: [name]
      vertex:{
          field:rowkey
      }
      batch: 256
      partition: 32
    }

  ]

  # 处理边数据
  edges: [
    # 设置边类型follow相关信息
    {
      # Nebula Graph中对应的边类型名称。
      name: follow

      type: {
        # 指定数据源文件格式，设置为HBase。
        source: hbase

        # 指定边数据导入Nebula Graph的方式，
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      host:192.168.*.*
      port:2181
      table:"follow"
      columnFamily:"cf"

      # 在fields里指定follow表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。
      fields: [degree]
      nebula.fields: [degree]

      # 在source里，将follow表中某一列作为边的起始点数据源。示例使用rowkey。
      # 在target里，将follow表中某一列作为边的目的点数据源。示例使用列dst_player。
      source:{
          field:rowkey
      }


      target:{
          field:dst_player
      }


      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }

    # 设置边类型serve相关信息
    {
      name: serve
      type: {
        source: hbase
        sink: client
      }
      host:192.168.*.*
      port:2181
      table:"serve"
      columnFamily:"cf"

      fields: [start_year,end_year]
      nebula.fields: [start_year,end_year]
      source:{
          field:rowkey
      }

      target:{
          field:teamid
      }

      batch: 256
      partition: 32
    }
  ]
}
```

### 步骤 3：向Nebula Graph导入数据

运行如下命令将HBase数据导入到Nebula Graph中。关于参数的说明，请参见[导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
${SPARK_HOME}/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.0.0.jar_path> -c <hbase_application.conf_path>
```

>**说明**：JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

示例：

```bash
${SPARK_HOME}/bin/spark-submit  --master "local" --class com.vesoft.nebula.exchange.Exchange  /root/nebula-spark-utils/nebula-exchange/target/nebula-exchange-2.0.0.jar  -c /root/nebula-spark-utils/nebula-exchange/target/classes/hbase_application.conf
```

您可以在返回信息中搜索`batchSuccess.<tag_name/edge_name>`，确认成功的数量。例如例如`batchSuccess.follow: 300`。

### 步骤 4：（可选）验证数据

您可以在Nebula Graph客户端（例如Nebula Graph Studio）中执行查询语句，确认数据是否已导入。例如：

```ngql
GO FROM "player100" OVER follow;
```

您也可以使用命令[`SHOW STATS`](https://docs.nebula-graph.com.cn/2.0/3.ngql-guide/7.general-query-statements/6.show/14.show-stats/)查看统计数据。

### 步骤 5：（如有）在Nebula Graph中重建索引

导入数据后，您可以在Nebula Graph中重新创建并重建索引。详情请参见[索引介绍](https://docs.nebula-graph.com.cn/2.0/3.ngql-guide/14.native-index-statements/)。