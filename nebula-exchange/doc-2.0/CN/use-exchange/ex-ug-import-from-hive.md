# 导入Hive数据

本文以一个示例说明如何使用Exchange将存储在Hive上的数据导入Nebula Graph。

## 数据集

本文以[basketballplayer数据集](https://docs-cdn.nebula-graph.com.cn/dataset/dataset.zip)为例。

在本示例中，该数据集已经存入Hive中名为`basketball`的数据库中，以`player`、`team`、`follow`和`serve`四个表存储了所有点和边的信息。以下为各个表的结构。

```sql
scala> sql("describe basketball.player").show
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|playerid|   string|   null|
|     age|   bigint|   null|
|    name|   string|   null|
+--------+---------+-------+

scala> sql("describe basketball.team").show
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|    teamid|   string|   null|
|      name|   string|   null|
+----------+---------+-------+

scala> sql("describe basketball.follow").show
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|src_player|   string|   null|
|dst_player|   string|   null|
|    degree|   bigint|   null|
+----------+---------+-------+

scala> sql("describe basketball.serve").show
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|  playerid|   string|   null|
|    teamid|   string|   null|
|start_year|   bigint|   null|
|  end_year|   bigint|   null|
+----------+---------+-------+
```

> **说明**：Hive的数据类型`bigint`与Nebula Graph的`int`对应。

## 环境配置

本文示例在MacOS下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.4.7，单机版

- Hadoop：2.9.2，伪分布式部署

- Hive：2.3.7，Hive Metastore 数据库为 MySQL 8.0.22

- Nebula Graph：2.0.0。使用[Docker Compose部署](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)。

## 前提条件

开始导入数据之前，您需要确认以下信息：

- 已经[安装部署Nebula Graph](https://docs.nebula-graph.com.cn/2.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/2.install-nebula-graph-by-rpm-or-deb/)并获取如下信息：

  - Graph服务和Meta服务的的IP地址和端口。

  - 拥有Nebula Graph写权限的用户名和密码。

- 已经编译Exchange。详情请参见[编译Exchange](../ex-ug-compile.md)。本示例中使用Exchange 2.0。

- 已经安装Spark。

- 了解Nebula Graph中创建Schema的信息，包括标签和边类型的名称、属性等。

- 已经安装并开启Hadoop服务，并已启动Hive Metastore数据库（本示例中为 MySQL）。

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

### 步骤 2：使用Spark SQL确认Hive SQL语句

启动spark-shell环境后，依次运行以下语句，确认Spark能读取Hive中的数据。

```sql
scala> sql("select playerid, age, name from basketball.player").show
scala> sql("select teamid, name from basketball.team").show
scala> sql("select src_player, dst_player, degree from basketball.follow").show
scala> sql("select playerid, teamid, start_year, end_year from basketball.serve").show
```

以下为表`basketball.player`中读出的结果。

```mysql
+---------+----+-----------------+
| playerid| age|             name|
+---------+----+-----------------+
|player100|  42|       Tim Duncan|
|player101|  36|      Tony Parker|
|player102|  33|LaMarcus Aldridge|
|player103|  32|         Rudy Gay|
|player104|  32|  Marco Belinelli|
+---------+----+-----------------+
...
```

### 步骤 3：修改配置文件

编译Exchange后，复制`target/classes/application.conf`文件设置Hive数据源相关的配置。在本示例中，复制的文件名为`hive_application.conf`。各个配置项的详细说明请参见[配置说明](../parameter-reference/ex-ug-parameter.md)。

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

  # 如果Spark和Hive部署在不同集群，才需要配置连接Hive的参数，否则请忽略这些配置。
  #hive: {
  #  waredir: "hdfs://NAMENODE_IP:9000/apps/svr/hive-xxx/warehouse/"
  #  connectionURL: "jdbc:mysql://your_ip:3306/hive_spark?characterEncoding=UTF-8"
  #  connectionDriverName: "com.mysql.jdbc.Driver"
  #  connectionUserName: "user"
  #  connectionPassword: "password"
  #}

  # Nebula Graph相关配置
  nebula: {
    address:{
      # 以下为Nebula Graph的Graph服务和所有Meta服务所在机器的IP地址及端口。
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
    {
      # Nebula Graph中对应的标签名称。
      name: player
      type: {
        # 指定数据源文件格式，设置为hive。
        source: hive
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 设置读取数据库basketball中player表数据的SQL语句
      exec: "select playerid, age, name from basketball.player"

      # 在fields里指定player表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。
      fields: [age,name]
      nebula.fields: [age,name]

      # 指定表中某一列数据为Nebula Graph中点VID的来源。
      # vertex.field的值必须与上述fields中的列名保持一致。
      vertex:{
        field:playerid
      }

      # 单批次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }
    # 设置标签team相关信息。
    {
      name: team
      type: {
        source: hive
        sink: client
      }
      exec: "select teamid, name from basketball.team"
      fields: [name]
      nebula.fields: [name]
      vertex: {
        field: teamid
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
        # 指定数据源文件格式，设置为hive。
        source: hive

        # 指定边数据导入Nebula Graph的方式，
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 设置读取数据库basketball中follow表数据的SQL语句。
      exec: "select src_player, dst_player, degree from basketball.follow"

      # 在fields里指定follow表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。
      fields: [degree]
      nebula.fields: [degree]

      # 在source里，将follow表中某一列作为边的起始点数据源。
      # 在target里，将follow表中某一列作为边的目的点数据源。
      source: {
        field: src_player
      }

      target: {
        field: dst_player
      }

      # 单批次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }

    # 设置边类型serve相关信息
    {
      name: serve
      type: {
        source: hive
        sink: client
      }
      exec: "select playerid, teamid, start_year, end_year from basketball.serve"
      fields: [start_year,end_year]
      nebula.fields: [start_year,end_year]
      source: {
        field: playerid
      }
      target: {
        field: teamid
      }
      batch: 256
      partition: 32
    }
  ]
}
```

### 步骤 4：向Nebula Graph导入数据

运行如下命令将Hive数据导入到Nebula Graph中。关于参数的说明，请参见[导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
<spark_install_path>/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.0.0.jar_path> -c <hive_application.conf_path> -h
```

>**说明**：JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

示例：

```bash
/usr/local/spark-2.4.7-bin-hadoop2.7/bin/spark-submit  --master "local" --class com.vesoft.nebula.exchange.Exchange  /root/nebula-spark-utils/nebula-exchange/target/nebula-exchange-2.0.0.jar  -c /root/nebula-spark-utils/nebula-exchange/target/classes/hive_application.conf -h
```

您可以在返回信息中搜索`batchSuccess.<tag_name/edge_name>`，确认成功的数量。例如例如`batchSuccess.follow: 300`。

### 步骤 5：（可选）验证数据

您可以在Nebula Graph客户端（例如Nebula Graph Studio）中执行查询语句，确认数据是否已导入。例如：

```ngql
GO FROM "player100" OVER follow;
```

您也可以使用命令[`SHOW STATS`](https://docs.nebula-graph.com.cn/2.0/3.ngql-guide/7.general-query-statements/6.show/14.show-stats/)查看统计数据。

### 步骤 6：（如有）在Nebula Graph中重建索引

导入数据后，您可以在Nebula Graph中重新创建并重建索引。详情请参见[索引介绍](https://docs.nebula-graph.com.cn/2.0/3.ngql-guide/14.native-index-statements/)。