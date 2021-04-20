# 导入Neo4j数据

本文以一个示例说明如何使用Exchange将存储在Neo4j的数据导入Nebula Graph。

## 实现方法

Exchange使用**Neo4j Driver 4.0.1**实现对Neo4j数据的读取。执行批量导出之前，您需要在配置文件中写入针对标签（label）和关系类型（Relationship Type）自动执行的Cypher语句，以及Spark分区数，提高数据导出性能。

Exchange读取Neo4j数据时需要完成以下工作：

1. Exchange中的Reader会将配置文件中`exec`部分的Cypher`RETURN`语句后面的语句替换为`COUNT(*)`，并执行这个语句，从而获取数据总量，再根据Spark分区数量计算每个分区的起始偏移量和大小。

2. （可选）如果用户配置了`check_point_path`目录，Reader会读取目录中的文件。如果处于续传状态，Reader会计算每个Spark分区应该有的偏移量和大小。

3. 在每个Spark分区里，Exchange中的Reader会在Cypher语句后面添加不同的`SKIP`和`LIMIT`语句，调用Neo4j Driver并行执行，将数据分布到不同的Spark分区中。

4. Reader最后将返回的数据处理成DataFrame。

至此，Exchange即完成了对Neo4j数据的导出。之后，数据被并行写入Nebula Graph数据库中。

整个过程如下图所示。

![Nebula Graph&reg; Exchange 从 Neo4j 数据库中导出数据再并行导入 Nebula Graph 数据库中](../figs/ex-ug-002.png "Nebula Graph&reg; Exchange 迁移 Neo4j 数据")

## 数据集

本文以[basketballplayer数据集](https://docs-cdn.nebula-graph.com.cn/dataset/dataset.zip)为例。

### 环境配置

本文示例在MacOS下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz

  - CPU 内核数：14

  - 内存：251 GB

- Spark：单机版，2.4.6 pre-build for Hadoop 2.7

- Neo4j：3.5.20 Community Edition

- Nebula Graph：2.0.0。使用[Docker Compose部署](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)。

## 前提条件

开始导入数据之前，您需要确认以下信息：

- 已经[安装部署Nebula Graph](https://docs.nebula-graph.com.cn/2.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/2.install-nebula-graph-by-rpm-or-deb/)并获取如下信息：

  - Graph服务和Meta服务的的IP地址和端口。

  - 拥有Nebula Graph写权限的用户名和密码。

- 已经编译Exchange。详情请参见[编译Exchange](../ex-ug-compile.md)。本示例中使用Exchange 2.0。

- 已经安装Spark。

- 了解Nebula Graph中创建Schema的信息，包括标签和边类型的名称、属性等。

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

### 步骤 2：配置源数据

为了提高Neo4j数据的导出速度，在Neo4j数据库中为相应属性创建索引。详细信息，参考[Neo4j用户手册](https://neo4j.com/docs/cypher-manual/current/query-tuning/indexes/)。

### 步骤 3：修改配置文件

编译Exchange后，复制`target/classes/application.conf`文件设置数据源相关的配置。在本示例中，复制的文件名为`neo4j_application.conf`。各个配置项的详细说明请参见[配置说明](../parameter-reference/ex-ug-parameter.md)。

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

    cores:{
      max: 16
    }
  }


  # Nebula Graph相关配置
  nebula: {
    address:{
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }
    user: root
    pswd: nebula
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
      name: player
      type: {
        source: neo4j
        sink: client
      }
      server: "bolt://192.168.*.*:7687"
      user: neo4j
      password:neo4j
      database:neo4j
      exec: "match (n:player) return n.id as id, n.age as age, n.name as name"
      fields: [age,name]
      nebula.fields: [age,name]
      vertex: {
        field:id
      }
      partition: 10
      batch: 1000
      check_point_path: /tmp/test
   }
  # 设置标签team相关信息。
  {
      name: team
      type: {
        source: neo4j
        sink: client
      }
      server: "bolt://192.168.*.*:7687"
      user: neo4j
      password:neo4j
      database:neo4j
      exec: "match (n:team) return n.id as id,n.name as name"
      fields: [name]
      nebula.fields: [name]
      vertex: {
        field:id
      }
      partition: 10
      batch: 1000
      check_point_path: /tmp/test
   }
  ]

  # 处理边数据
  edges: [
    # 设置边类型follow相关信息
    {
      name: follow
      type: {
        source: neo4j
        sink: client
      }
      server: "bolt://192.168.*.*:7687"
      user: neo4j
      password:neo4j
      database:neo4j
      exec: "match (a:player)-[r:follow]->(b:player) return a.id as src, b.id as dst, r.degree as degree  order by id(r)"
      fields: [degree]
      nebula.fields: [degree]
      source: {
        field: src
      }
      target: {
        field: dst
      }
      partition: 10
      batch: 1000
      check_point_path: /tmp/test
    }
   # 设置边类型serve相关信息
   {
      name: serve
      type: {
        source: neo4j
        sink: client
      }
      server: "bolt://192.168.*.*:7687"
      user: neo4j
      password:neo4j
      database:neo4j
      exec: "match (a:player)-[r:serve]->(b:team) return a.id as src, b.id as dst, r.start_year as start_year, r.end_year as end_year  order by id(r)"
      fields: [start_year,end_year]
      nebula.fields: [start_year,end_year]
      source: {
        field: src
      }
      target: {
        field: dst
      }
      partition: 10
      batch: 1000
      check_point_path: /tmp/test
    }
   ]
}
```

#### exec配置说明

在配置`tags.exec`或者`edges.exec`参数时，需要填写Cypher查询语句。为了保证每次查询结果排序一致，并且为了防止在导入时丢失数据，强烈建议您在Cypher查询语句中加入`ORDER BY`子句，同时，为了提高数据导入效率，最好选取有索引的属性作为排序的属性。如果没有索引，您也可以观察默认的排序，选择合适的属性用于排序，以提高效率。如果默认的排序找不到规律，您可以根据点或关系的ID进行排序，并且将`partition`设置为一个尽量小的值，减轻Neo4j的排序压力。

>**说明**：使用`ORDER BY`子句会延长数据导入的时间。

另外，Exchange需要在不同Spark分区执行不同`SKIP`和`LIMIT`的Cypher语句，所以在`tags.exec`和`edges.exec`对应的Cypher语句中不能含有`SKIP`和`LIMIT`子句。

#### tags.vertex或edges.vertex配置说明

Nebula Graph在创建点和边时会将ID作为唯一主键，如果主键已存在则会覆盖该主键中的数据。所以，假如将某个Neo4j属性值作为Nebula Graph的ID，而这个属性值在Neo4j中是有重复的，就会导致重复ID，它们对应的数据有且只有一条会存入Nebula Graph中，其它的则会被覆盖掉。由于数据导入过程是并发地往Nebula Graph中写数据，最终保存的数据并不能保证是Neo4j中最新的数据。

#### check_point_path配置说明

如果启用了断点续传功能，为避免数据丢失，在断点和续传之间，数据库不应该改变状态，例如不能添加数据或删除数据，同时，不能更改`partition`数量配置。

### 步骤 4：向Nebula Graph导入数据

运行如下命令将文件数据导入到Nebula Graph中。关于参数的说明，请参见[导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
${SPARK_HOME}/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.0.0.jar_path> -c <neo4j_application.conf_path> 
```

>**说明**：JAR包有两种获取方式：[自行编译](../ex-ug-compile.md)或者从maven仓库下载。

示例：

```bash
${SPARK_HOME}/bin/spark-submit  --master "local" --class com.vesoft.nebula.exchange.Exchange  /root/nebula-spark-utils/nebula-exchange/target/nebula-exchange-2.0.0.jar  -c /root/nebula-spark-utils/nebula-exchange/target/classes/neo4j_application.conf
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
