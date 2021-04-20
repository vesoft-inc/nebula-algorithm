# 导入JSON文件数据

本文以一个示例说明如何使用Exchange将存储在HDFS上的JSON文件数据导入Nebula Graph。

## 数据集

本文以basketballplayer数据集为例。部分示例数据如下：

- player

  ```json
  {"id":"player100","age":42,"name":"Tim Duncan"}
  {"id":"player101","age":36,"name":"Tony Parker"}
  {"id":"player102","age":33,"name":"LaMarcus Aldridge"}
  {"id":"player103","age":32,"name":"Rudy Gay"}
  ...
  ```

- team

  ```json
  {"id":"team200","name":"Warriors"}
  {"id":"team201","name":"Nuggets"}
  ...
  ```

- follow

  ```json
  {"src":"player100","dst":"player101","degree":95}
  {"src":"player101","dst":"player102","degree":90}
  ...
  ```

- serve

  ```json
  {"src":"player100","dst":"team204","start_year":"1997","end_year":"2016"}
  {"src":"player101","dst":"team204","start_year":"1999","end_year":"2018"}
  ...
  ```

## 环境配置

本文示例在MacOS下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.3.0，单机版

- Hadoop：2.9.2，伪分布式部署

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

分析文件中的数据，按以下步骤在Nebula Graph中创建Schema：

1. 确认Schema要素。Nebula Graph中的Schema要素如下表所示。

    | 要素  | 名称 | 属性 |
    | :--- | :--- | :--- |
    | 标签（Tag） | `player` | `name string, age int` |
    | 标签（Tag） | `team` | `name string` |
    | 边类型（Edge Type） | `follow` | `degree int` |
    | 边类型（Edge Type） | `serve` | `start_year int, end_year int` |

2. 使用Nebula Console创建一个图空间**basketballplayer**，并创建一个Schema，如下所示。

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

### 步骤 2：处理JSON文件

确认以下信息：

1. 处理JSON文件以满足Schema的要求。

2. JSON文件必须存储在HDFS中，并已获取文件存储路径。

### 步骤 3. 修改配置文件

编译Exchange后，复制`target/classes/application.conf`文件设置JSON数据源相关的配置。在本示例中，复制的文件名为`json_application.conf`。各个配置项的详细说明请参见[配置说明](../parameter-reference/ex-ug-parameter.md)。

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
    executor: {
        memory:1G
    }

    cores {
      max: 16
    }
  }

  # Nebula Graph相关配置
  nebula: {
    address:{
      # 指定Graph服务和所有Meta服务的IP地址和端口。
      # 如果有多台服务器，地址之间用英文逗号（,）分隔。
      # 格式: "ip1:port","ip2:port","ip3:port"
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }

    # 指定拥有Nebula Graph写权限的用户名和密码。
    user: root
    pswd: nebula

    # 指定图空间名称。
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
      # 指定Nebula Graph中定义的标签名称。
      name: player
      type: {
        # 指定数据源，使用JSON。
        source: json

        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 指定JSON文件的HDFS路径。
      # 用双引号括起路径，以hdfs://开头。
      path: "hdfs://192.168.*.*:9000/data/vertex_player.json"

      # 在fields里指定JSON文件中key名称，其对应的value会作为Nebula Graph中指定属性的数据源。
      # 如果需要指定多个值，用英文逗号（,）隔开。
      fields: [age,name]

      # 指定Nebula Graph中定义的属性名称。
      # fields与nebula.fields的顺序必须一一对应。
      nebula.fields: [age, name]

      # 指定一个列作为VID的源。
      # vertex的值必须与JSON文件中的字段保持一致。
      # 目前，Nebula Graph 2.0.0仅支持字符串或整数类型的VID。
      # 不要使用vertex.policy映射。
      vertex: {
        field:id
      }

      # 指定单批次写入Nebula Graph的最大点数量。
      batch: 256

      # 指定Spark分片数量。
      partition: 32
    }

    # 设置标签team相关信息。
    {
      # 指定Nebula Graph中定义的标签名称。
      name: team
      type: {
        # 指定数据源，使用JSON。
        source: json

        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 指定JSON文件的HDFS路径。
      # 用双引号括起路径，以hdfs://开头。
      path: "hdfs://192.168.*.*:9000/data/vertex_team.json"

      # 在fields里指定JSON文件中key名称，其对应的value会作为Nebula Graph中指定属性的数据源。
      # 如果需要指定多个值，用英文逗号（,）隔开。
      fields: [name]

      # 指定Nebula Graph中定义的属性名称。
      # fields与nebula.fields的顺序必须一一对应。
      nebula.fields: [name]

      # 指定一个列作为VID的源。
      # vertex的值必须与JSON文件中的字段保持一致。
      # 目前，Nebula Graph 2.0.0仅支持字符串或整数类型的VID。
      # 不要使用vertex.policy映射。
      vertex: {
        field:id
      }


      # 指定单批次写入Nebula Graph的最大点数量。
      batch: 256

      # 指定Spark分片数量。
      partition: 32
    }


    # 如果需要添加更多点，请参考前面的配置进行添加。
  ]
  # 处理边
  edges: [
    # 设置边类型follow相关信息。
    {
      # 指定Nebula Graph中定义的边类型名称。
      name: follow
      type: {
        # 指定数据源，使用JSON。
        source: json

        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 指定JSON文件的HDFS路径。
      # 用双引号括起路径，以hdfs://开头。
      path: "hdfs://192.168.*.*:9000/data/edge_follow.json"

      # 在fields里指定JSON文件中key名称，其对应的value会作为Nebula Graph中指定属性的数据源。
      # 如果需要指定多个值，用英文逗号（,）隔开。
      fields: [degree]

      # 指定Nebula Graph中定义的属性名称。
      # fields与nebula.fields的顺序必须一一对应。
      nebula.fields: [degree]

      # 指定一个列作为起始点和目的点的源。
      # vertex的值必须与JSON文件中的字段保持一致。
      # 目前，Nebula Graph 2.0.0仅支持字符串或整数类型的VID。
      # 不要使用vertex.policy映射。
      source: {
        field: src
      }
      target: {
        field: dst
      }


      # 指定一个列作为rank的源(可选)。
      #ranking: _c4

      # 指定单批次写入Nebula Graph的最大边数量。
      batch: 256

      # 指定Spark分片数量。
      partition: 32
    }

    # 设置边类型serve相关信息。
    {
      # 指定Nebula Graph中定义的边类型名称。
      name: serve
      type: {
        # 指定数据源，使用JSON。
        source: json

        # 指定如何将点数据导入Nebula Graph：Client或SST。
        sink: client
      }

      # 指定JSON文件的HDFS路径。
      # 用双引号括起路径，以hdfs://开头。
      path: "hdfs://192.168.*.*:9000/data/edge_serve.json"

      # 在fields里指定JSON文件中key名称，其对应的value会作为Nebula Graph中指定属性的数据源。
      # 如果需要指定多个值，用英文逗号（,）隔开。
      fields: [start_year,end_year]

      # 指定Nebula Graph中定义的属性名称。
      # fields与nebula.fields的顺序必须一一对应。
      nebula.fields: [start_year, end_year]

      # 指定一个列作为起始点和目的点的源。
      # vertex的值必须与JSON文件中的字段保持一致。
      # 目前，Nebula Graph 2.0.0仅支持字符串或整数类型的VID。
      # 不要使用vertex.policy映射。
      source: {
        field: src
      }
      target: {
        field: dst
      }


      # 指定一个列作为rank的源(可选)。
      #ranking: _c5


      # 指定单批次写入Nebula Graph的最大边数量。
      batch: 256

      # 指定Spark分片数量。
      partition: 32
    }

  ]
  # 如果需要添加更多边，请参考前面的配置进行添加。
}
```

### 步骤 4：向Nebula Graph导入数据

运行如下命令将JSON文件数据导入到Nebula Graph中。关于参数的说明，请参见[导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
<spark_install_path>/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.0.0.jar_path> -c <json_application.conf_path> 
```

>**说明**：JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

示例：

```bash
/usr/local/spark-2.4.7-bin-hadoop2.7/bin/spark-submit  --master "local" --class com.vesoft.nebula.exchange.Exchange  /root/nebula-spark-utils/nebula-exchange/target/nebula-exchange-2.0.0.jar  -c /root/nebula-spark-utils/nebula-exchange/target/classes/json_application.conf
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
