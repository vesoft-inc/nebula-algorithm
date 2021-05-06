# 导入MySQL数据

本文以一个示例说明如何使用Exchange将存储在MySQL上的数据导入Nebula Graph。

## 数据集

本文以[basketballplayer数据集](https://docs-cdn.nebula-graph.com.cn/dataset/dataset.zip)为例。

在本示例中，该数据集已经存入MySQL中名为`basketball`的数据库中，以`player`、`team`、`follow`和`serve`四个表存储了所有点和边的信息。以下为各个表的结构。

```sql
mysql> desc player;
+----------+-------------+------+-----+---------+-------+
| Field    | Type        | Null | Key | Default | Extra |
+----------+-------------+------+-----+---------+-------+
| playerid | varchar(30) | YES  |     | NULL    |       |
| age      | int         | YES  |     | NULL    |       |
| name     | varchar(30) | YES  |     | NULL    |       |
+----------+-------------+------+-----+---------+-------+

mysql> desc team;
+--------+-------------+------+-----+---------+-------+
| Field  | Type        | Null | Key | Default | Extra |
+--------+-------------+------+-----+---------+-------+
| teamid | varchar(30) | YES  |     | NULL    |       |
| name   | varchar(30) | YES  |     | NULL    |       |
+--------+-------------+------+-----+---------+-------+

mysql> desc follow;
+------------+-------------+------+-----+---------+-------+
| Field      | Type        | Null | Key | Default | Extra |
+------------+-------------+------+-----+---------+-------+
| src_player | varchar(30) | YES  |     | NULL    |       |
| dst_player | varchar(30) | YES  |     | NULL    |       |
| degree     | int         | YES  |     | NULL    |       |
+------------+-------------+------+-----+---------+-------+

mysql> desc serve;
+------------+-------------+------+-----+---------+-------+
| Field      | Type        | Null | Key | Default | Extra |
+------------+-------------+------+-----+---------+-------+
| playerid   | varchar(30) | YES  |     | NULL    |       |
| teamid     | varchar(30) | YES  |     | NULL    |       |
| start_year | int         | YES  |     | NULL    |       |
| end_year   | int         | YES  |     | NULL    |       |
+------------+-------------+------+-----+---------+-------+
```

## 环境配置

本文示例在MacOS下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.4.7，单机版

- Hadoop：2.9.2，伪分布式部署

- MySQL： 8.0.23

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

编译Exchange后，复制`target/classes/application.conf`文件设置MySQL数据源相关的配置。在本示例中，复制的文件名为`mysql_application.conf`。各个配置项的详细说明请参见[配置说明](../parameter-reference/ex-ug-parameter.md)。

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
    {
      # Nebula Graph中对应的标签名称。
      name: player
      type: {
        # 指定数据源文件格式，设置为MySQL。
        source: mysql
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      host:192.168.*.*
      port:3306
      database:"basketball"
      table:"player"
      user:"test"
      password:"123456"
      sentence:"select playerid, age, name from basketball.player order by playerid;"

      # 在fields里指定player表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。
      fields: [age,name]
      nebula.fields: [age,name]

      # 指定表中某一列数据为Nebula Graph中点VID的来源。
      # vertex.field的值必须与上述fields中的列名保持一致。
      vertex: {
        field:playerid
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
        source: mysql
        sink: client
      }

      host:192.168.*.*
      port:3306
      database:"basketball"
      table:"team"
      user:"test"
      password:"123456"
      sentence:"select teamid, name from basketball.team order by teamid;"

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
        # 指定数据源文件格式，设置为MySQL。
        source: mysql

        # 指定边数据导入Nebula Graph的方式，
        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      host:192.168.*.*
      port:3306
      database:"basketball"
      table:"follow"
      user:"test"
      password:"123456"
      sentence:"select src_player,dst_player,degree from basketball.follow order by src_player;"

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

      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }

    # 设置边类型serve相关信息
    {
      name: serve
      type: {
        source: mysql
        sink: client
      }

      host:192.168.*.*
      port:3306
      database:"basketball"
      table:"serve"
      user:"test"
      password:"123456"
      sentence:"select playerid,teamid,start_year,end_year from basketball.serve order by playerid;"
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

### 步骤 3：向Nebula Graph导入数据

运行如下命令将MySQL数据导入到Nebula Graph中。关于参数的说明，请参见[导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
${SPARK_HOME}/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.0.0.jar_path> -c <mysql_application.conf_path>
```

>**说明**：JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

示例：

```bash
${SPARK_HOME}/bin/spark-submit  --master "local" --class com.vesoft.nebula.exchange.Exchange  /root/nebula-spark-utils/nebula-exchange/target/nebula-exchange-2.0.0.jar  -c /root/nebula-spark-utils/nebula-exchange/target/classes/mysql_application.conf
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
