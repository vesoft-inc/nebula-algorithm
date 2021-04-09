# 导入 HIVE 数据

本文以一个示例说明如何使用 Exchange 将存储在 HIVE 的数据导入 Nebula Graph。

## 数据集

本文以美国 Stanford Network Analysis Platform (SNAP) 提供的 [Social Network: MOOC User Action Dataset](https://snap.stanford.edu/data/act-mooc.html "点击前往 Stanford Network Analysis Platform (SNAP) 网站") 以及由公开网络上获取的不重复的 97 个课程名称作为示例数据集，包括：

- 两类点（`user` 和 `course`），共计 7,144 个点。
- 一种关系（`action`），共计 411,749 条边。

详细的数据集，您可以从 [nebula-web-docker](https://github.com/vesoft-inc/nebula-web-docker/tree/master/example/mooc-actions "点击前往 GitHub") 仓库中下载。

在本示例中，该数据集已经存入 HIVE 中名为 `mooc` 的数据库中，以 `users`、`courses` 和 `actions` 三个表存储了所有点和边的信息。以下为各个表的结构。

```sql
scala> sql("describe mooc.users").show
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|  userid|   bigint|   null|
+--------+---------+-------+

scala> sql("describe mooc.courses").show
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|  courseid|   bigint|   null|
|coursename|   string|   null|
+----------+---------+-------+

scala> sql("describe mooc.actions").show
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|actionid|   bigint|   null|
|   srcid|   bigint|   null|
|   dstid|   string|   null|
|duration|   double|   null|
|feature0|   double|   null|
|feature1|   double|   null|
|feature2|   double|   null|
|feature3|   double|   null|
|   label|  boolean|   null|
+--------+---------+-------+
```

> **说明**：Hive 的 `bigint` 与 Nebula Graph 的 `int` 对应。

## 环境配置

本文示例在 MacOS 下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.4.7，单机版

- Hadoop：2.9.2，伪分布式部署

- HIVE：2.3.7，Hive Metastore 数据库为 MySQL 8.0.22

- Nebula Graph：V1.2.0，使用 Docker Compose 部署。详细信息，参考 [使用 Docker Compose 部署 Nebula Graph](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)

## 前提条件

开始导入数据之前，您需要确认以下信息：

- 已经完成 Exchange 编译。详细信息，参考 [编译 Exchange](../ex-ug-compile.md)。本示例中使用 Exchange v1.1.0。

- 已经安装 Spark。

- 已经安装并开启 Hadoop 服务，并已启动 Hive Metastore 数据库（本示例中为 MySQL）。

- 已经部署并启动 Nebula Graph，并获取：
  - Graph 服务、Meta 服务所在机器的 IP 地址和端口信息。
  - Nebula Graph 数据库的拥有写权限的用户名及其密码。

- 在 Nebula Graph 中创建图数据模式（Schema）所需的所有信息，包括标签和边类型的名称、属性等。

## 操作步骤

### 步骤 1. 在 Nebula Graph 中创建 Schema

按以下步骤在 Nebula Graph 中创建 Schema：

1. 确认 Schema 要素：Nebula Graph 中的 Schema 要素如下表所示。

    | 要素  | 名称 | 属性 |
    | :--- | :--- | :--- |
    | 标签（Tag） | `user` | `userId int` |
    | 标签（Tag） | `course` | `courseId int, courseName string` |
    | 边类型（Edge Type） | `action` | `actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double` |

2. 在 Nebula Graph 里创建一个图空间 **hive**，并创建一个 Schema，如下所示。

    ```ngql
    -- 创建图空间
    CREATE SPACE hive(partition_num=10, replica_factor=1);
    
    -- 选择图空间 hive
    USE hive;
    
    -- 创建标签 user
    CREATE TAG user(userId int);
    
    -- 创建标签 course
    CREATE TAG course(courseId int, courseName string);
    
    -- 创建边类型 action
    CREATE EDGE action (actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double);
    ```

关于 Nebula Graph 构图的更多信息，参考《Nebula Graph Database 手册》的 [快速开始](../../manual-CN/1.overview/2.quick-start/1.get-started/ "点击前往 Nebula Graph 网站")。

### 步骤 2. 使用 Spark SQL 确认 HIVE SQL 语句

启动 spark-shell 环境后，依次运行以下语句，确认 Spark 能读取 HIVE 中的数据。

```sql
scala> sql("select userid from mooc.users").show
scala> sql("select courseid, coursename from mooc.courses").show
scala> sql("select actionid, srcid, dstid, duration, feature0, feature1, feature2, feature3, label from mooc.actions").show
```

以下为 `mooc.actions` 表中读出的结果。

```mysql
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
|actionid|srcid|               dstid|duration|    feature0|    feature1|   feature2|   feature3|label|
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
|       0|    0|Environmental Dis...|     0.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       1|    0|  History of Ecology|     6.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       2|    0|      Women in Islam|    41.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       3|    0|  History of Ecology|    49.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       4|    0|      Women in Islam|    51.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       5|    0|Legacies of the A...|    55.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       6|    0|          ITP Core 2|    59.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       7|    0|The Research Pape...|    62.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       8|    0|        Neurobiology|    65.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       9|    0|           Wikipedia|   113.0|-0.319991479|-0.435701433|1.108826104|12.77723482|false|
|      10|    0|Media History and...|   226.0|-0.319991479|-0.435701433|0.607804941|149.4512115|false|
|      11|    0|             WIKISOO|   974.0|-0.319991479|-0.435701433|1.108826104|3.344522776|false|
|      12|    0|Environmental Dis...|  1000.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      13|    0|             WIKISOO|  1172.0|-0.319991479|-0.435701433|1.108826104|1.136866766|false|
|      14|    0|      Women in Islam|  1182.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      15|    0|  History of Ecology|  1185.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      16|    0|Human Development...|  1687.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      17|    1|Human Development...|  7262.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      18|    1|  History of Ecology|  7266.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      19|    1|      Women in Islam|  7273.0|-0.319991479|-0.435701433|0.607804941|0.936170765|false|
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
only showing top 20 rows
```

### 步骤 3. 修改配置文件

完成 Exchange 编译后，进入 `nebula-java/tools/exchange` 目录，根据 `target/classes/application.conf` 文件修改 HIVE 数据源相关的配置文件。在本示例中，文件被重命名为 `hive_application.conf`。以下仅详细说明点和边数据的配置信息，本次示例中未使用的配置项已被注释，但是提供了配置说明。Spark 和 Nebula Graph 相关配置，参考 [Spark 参数](../parameter-reference/ex-ug-paras-spark.md)和 [Nebula Graph 参数](../parameter-reference/ex-ug-paras-nebulagraph.md)。

```conf
{
  # Spark 相关配置
  spark: {
    app: {
      name: Spark Writer
    }
    driver: {
      cores: 1
      maxResultSize: 1G
    }
    cores {
      max: 16
    }
  }
  # Nebula Graph 相关配置
  nebula: {
    address:{
      # 以下为 Nebula Graph 的 Graph 服务和 Meta 服务所在机器的 IP 地址及端口
      # 如果有多个地址，格式为 "ip1:port","ip2:port","ip3:port"
      # 不同地址之间以英文逗号 (,) 隔开
      graph:["127.0.0.1:3699"]
      meta:["127.0.0.1:45500"]
    }
    # 填写的账号必须拥有 Nebula Graph 相应图空间的写数据权限
    user: user
    pswd: password
    # 填写 Nebula Graph 中需要写入数据的图空间名称
    space: hive
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
  # 处理标签
  tags: [
    # 设置标签相关信息
    {
      # Nebula Graph 中对应的标签名称。
      name: user
      type: {
        # 指定数据源文件格式，设置为 hive。
        source: hive
        # 指定点数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }

      # 设置读取数据库 mooc 中 users 表数据的 SQL 语句
      exec: "select userid from mooc.users"

      # 在 fields 里指定 users 表中的列名称，其对应的 value
      # 会作为 Nebula Graph 中指定属性 userId (nebula.fields) 的数据源
      # fields 和 nebula.fields 里的配置必须一一对应
      # 如果需要指定多个列名称，用英文逗号（,）隔开
      fields: [userid]
      nebula.fields: [userId]

      # 指定表中某一列数据为 Nebula Graph 中点 VID 的来源。
      # vertex.field 的值必须与上述 fields 中的列名保持一致。
      # 如果数据不是 int 类型，则添加 vertex.policy 指定 VID 映射策略，建议设置为 "hash"，参考以下 course 标签的设置。
      vertex: userid

      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32

      # isImplicit 的设置说明参考：https://github.com/vesoft-inc/nebula-java/
      # blob/v1.0/tools/exchange/src/main/resources/application.conf
      isImplicit: true
    }
    {
      name: course
      type: {
        source: hive
        sink: client
      }
      exec: "select courseid, coursename from mooc.courses"
      fields: [courseid, coursename]
      nebula.fields: [courseId, courseName]

      # 指定表中某一列数据为 Nebula Graph 中点 VID 的来源。
      # vertex.field 的值必须与上述 fields 中的列名保持一致。
      # 如果数据不是 int 类型，则添加 vertex.policy 指定 VID 映射策略，建议设置为 "hash"。
      vertex: {
        field: coursename
        policy: "hash"
      }
      batch: 256
      partition: 32
      isImplicit: true
    }

  ]

  # 处理边数据
  edges: [
    # 设置边类型 action 相关信息
    {
      # Nebula Graph 中对应的边类型名称。
      name: action

      type: {
        # 指定数据源文件格式，设置为 hive。
        source: hive

        # 指定边数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }

      # 设置读取数据库 mooc 中 actions 表数据的 SQL 语句
      exec: "select actionid, srcid, dstid, duration, feature0, feature1, feature2, feature3, label from mooc.actions"

      # 在 fields 里指定 actions 表中的列名称，其对应的 value
      # 会作为 Nebula Graph 中 action 的属性（nebula.fields）值来源
      # fields 和 nebula.fields 里的配置必须一一对应
      # 如果需要指定多个列名称，用英文逗号（,）隔开
      fields: [actionid, duration, feature0, feature1, feature2, feature3, label]
      nebula.fields: [actionId, duration, feature0, feature1, feature2, feature3, label]

      # 在 source 里，将 actions 表中某一列作为边起点数据源
      # 在 target 里，将 actions 表中某一列作为边终点数据源
      # 如果数据源是 int 或 long 类型，直接指定列名
      # 如果数据源不是 int 类型，则添加 vertex.policy 指定 VID 映射策略，建议设置为 "hash"
      source: srcid
      target: {
        field: dstid
        policy: "hash"
      }

      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32
    }
  ]
}
```

### 步骤 4. （可选）检查配置文件是否正确

完成配置后，运行以下命令检查配置文件格式是否正确。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.1.0.jar -c /path/to/conf/hive_application.conf -h -D
```

### 步骤 5. 向 Nebula Graph 导入数据

运行以下命令将 HIVE 中的数据导入到 Nebula Graph 中。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.1.0.jar -c /path/to/conf/hive_application.conf -h
```

### 步骤 6. （可选）验证数据

您可以在 Nebula Graph 客户端（例如 Nebula Graph Studio）里执行语句，确认数据是否已导入，例如：

```ngql
GO FROM 1 OVER action;
```

如果返回边终点（`action._dst`）即表明数据已导入。

您也可以使用 db_dump 工具统计数据是否已经全部导入。详细的使用信息参考 [Dump Tool](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/5.storage-service-administration/data-export/dump-tool/)。

### 步骤 7. （可选）在 Nebula Graph 中重构索引

导入数据后，您可以在 Nebula Graph 中重新创建并重构索引。详细信息，参考[《Nebula Graph Database 手册》](https://docs.nebula-graph.com.cn/manual-CN/2.query-language/4.statement-syntax/1.data-definition-statements/ "点击前往 Nebula Graph 网站")。
