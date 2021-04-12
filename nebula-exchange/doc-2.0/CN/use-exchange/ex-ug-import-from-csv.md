# 导入 CSV 文件数据

本文以一个示例说明如何使用 Exchange 将存储在 HDFS 上的 CSV 文件数据导入 Nebula Graph。

如果您要向 Nebula Graph 导入本地 CSV 文件，参考 [CSV 文件导入示例](../../manual-CN/1.overview/2.quick-start/4.import-csv-file.md)。

## 数据集

本文以美国 Stanford Network Analysis Platform (SNAP) 提供的 [Social Network: MOOC User Action Dataset](https://snap.stanford.edu/data/act-mooc.html "点击前往 Stanford Network Analysis Platform (SNAP) 网站") 以及由公开网络上获取的不重复的 97 个课程名称作为示例数据集，包括：

- 两类点（`user` 和 `course`），共计 7,144 个点。
- 一种关系（`action`），共计 411,749 条边。

详细的数据集，您可以从 [nebula-web-docker](https://github.com/vesoft-inc/nebula-web-docker/tree/master/example/mooc-actions "点击前往 GitHub") 仓库中下载。

## 环境配置

本文示例在 MacOS 下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.3.0，单机版

- Hadoop：2.9.2，伪分布式部署

- Nebula Graph：V1.1.0，使用 Docker Compose 部署。详细信息，参考 [使用 Docker Compose 部署 Nebula Graph](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)

## 前提条件

开始导入数据之前，您需要确认以下信息：

- 已经完成 Exchange 编译。详细信息，参考 [编译 Exchange](../ex-ug-compile.md)。本示例中使用 Exchange v1.1.0。

- 已经安装 Spark。

- 已经安装并开启 Hadoop 服务。

- 已经部署并启动 Nebula Graph，并获取：
  - Graph 服务、Meta 服务所在机器的 IP 地址和端口信息。
  - Nebula Graph 数据库的拥有写权限的用户名及其密码。

- 在 Nebula Graph 中创建图数据模式（Schema）所需的所有信息，包括标签和边类型的名称、属性等。

## 操作步骤

### 步骤 1. 在 Nebula Graph 中创建 Schema

分析 CSV 文件中的数据，按以下步骤在 Nebula Graph 中创建 Schema：

1. 确认 Schema 要素：Nebula Graph 中的 Schema 要素如下表所示。

    | 要素  | 名称 | 属性 |
    | :--- | :--- | :--- |
    | 标签（Tag） | `user` | `userId int` |
    | 标签（Tag） | `course` | `courseId int, courseName string` |
    | 边类型（Edge Type） | `action` | `actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double` |

2. 在 Nebula Graph 里创建一个图空间 **csv**，并创建一个 Schema，如下所示。

    ```ngql
    -- 创建图空间
    CREATE SPACE csv(partition_num=10, replica_factor=1);
    
    -- 选择图空间 csv
    USE csv;
    
    -- 创建标签 user
    CREATE TAG user(userId int);
    
    -- 创建标签 course
    CREATE TAG course(courseId int, courseName string);
    
    -- 创建边类型 action
    CREATE EDGE action (actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double);
    ```

关于 Nebula Graph 构图的更多信息，参考《Nebula Graph Database 手册》的 [快速开始](../../manual-CN/1.overview/2.quick-start/1.get-started/ "点击前往 Nebula Graph 网站")。

### 步骤 2. 处理 CSV 文件

确认以下信息：

1. CSV 文件已经根据 Schema 作了处理。详细操作请参考 [Nebula Graph Studio 快速开始](../../nebula-studio/quick-start/st-ug-prepare-csv.md)。
   > **说明**：Exchange 支持上传有表头或者无表头的 CSV 文件。

2. CSV 文件必须存储在 HDFS 中，并已获取文件存储路径。

### 步骤 3. 修改配置文件

完成 Exchange 编译后，进入 `nebula-java/tools/exchange` 目录，根据 `target/classes/application.conf` 文件修改 CSV 数据源相关的配置文件。在本示例中，文件被重命名为 `csv_application.conf`。以下仅详细说明点和边数据的配置信息，本次示例中未使用的配置项已被注释，但是提供了配置说明。Spark 和 Nebula Graph 相关配置，参考 [Spark 参数](../parameter-reference/ex-ug-paras-spark.md)和 [Nebula Graph 参数](../parameter-reference/ex-ug-paras-nebulagraph.md)。

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
    space: csv
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
    # 设置标签 course 相关信息
    {
      # Nebula Graph 中对应的标签名称。
      name: course
      type: {
        # 指定数据源文件格式，设置为 csv。
        source: csv
        # 指定点数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }
      # CSV 文件所在的 HDFS 路径，String 类型，必须以 hdfs:// 开头。
      path: "hdfs://namenode_ip:port/path/to/course.csv"
      
      # 如果 CSV 文件里不带表头，则写入 [_c0, _c1, _c2, ... _cn]，
      # 表示 CSV 文件中的数据列名，作为 course 各属性值来源。
      # 如果 CSV 文件里有表头，则写入各列名。
      # fields 与 nebula.fields 的顺序必须一一对应。
      fields: [_c0, _c1]

      # 设置 Nebula Graph 中与 CSV 文件各列对应的属性名称，
      # fields 与 nebula.fields 的顺序必须一一对应。
      nebula.fields: [courseId, courseName]

      # Exchange 1.1.0 添加了 csv.fields 参数：
      # 如果配置了 csv.fields，无论 CSV 文件是否有表头，
      # fields 的配置必须与 csv.fields 的配置保持一致。
      # csv.fields: [courseId, courseName]

      # 指定 CSV 中的某一列数据为 Nebula Graph 中点 VID 的来源。
      # vertex.field 的值必须与上述 fields 或者 csv.fields 中的列名保持一致。
      # 如果数据不是 int 类型，则添加 vertex.policy 指定 VID 映射策略，建议设置为 "hash"。
      vertex: {
        field: _c1,
        policy: "hash"
      }

      # 标明数据源中数据分隔方式，默认为英文逗号（,）。
      separator: ","

      # 如果 CSV 文件中有表头，header 设置为 true。
      # 如果 CSV 文件中没有表头，header 设置为 false（默认值）。
      header: false

      # 单次写入 Nebula Graph 的最大点数据量。
      batch: 256

      # Spark 分区数量
      partition: 32

      # isImplicit 的设置说明参考：https://github.com/vesoft-inc/nebula-java/
      # blob/v1.0/tools/exchange/src/main/resources/application.conf
      isImplicit: true
    }

    # 设置标签 user 相关信息
    {
      name: user
      type: {
        source: csv
        sink: client
      }
      path: "hdfs://namenode_ip:port/path/to/user.csv"

      # Exchange 1.1.0 添加了 csv.fields 参数
      # 如果 CSV 文件里不带表头，但是配置了 csv.fields，
      # 则 fields 的配置必须与 csv.fields 保持一致，
      # Exchange 会将 csv.fields 里的设置作为表头。
      # fields 与 nebula.fields 的顺序必须一一对应。
      fields: [userId]

      # 设置 Nebula Graph 中与 CSV 文件各列对应的属性名称，
      # 必须与 fields 或者 csv.fields 的顺序一一对应。
      nebula.fields: [userId]

      # 如果配置了 csv.fields，无论 CSV 文件是否有表头，
      # 均以这个参数指定的名称作为表头，
      # fields 的配置必须与 csv.fields 的配置保持一致，
      # 同时，vertex 的设置必须与 csv.fields 的设置相同。
      csv.fields: [userId]

      # vertex 的值必须与 fields 或者 csv.fields 中相应的列名保持一致。
      vertex: userId
      separator: ","
      header: false
      batch: 256
      partition: 32

      # isImplicit 设置说明，详见 https://github.com/vesoft-inc/nebula-java/blob
      # /v1.0/tools/exchange/src/main/resources/application.conf
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
        # 指定数据源文件格式，设置为 csv。
        source: csv

        # 指定边数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }

      # 指定 CSV 文件所在的 HDFS 路径，String 类型，必须以 hdfs:// 开头。
      path: "hdfs://namenode_ip:port/path/to/actions.csv"

      # 如果 CSV 文件里不带表头，
      # 则写入 [_c0, _c1, _c2, ... _cn]，
      # 依次表示 CSV 文件中各数据列，作为 action 各属性值来源。
      # 如果 CSV 文件里有表头，则写入各列名。
      # fields 与 nebula.fields 的顺序必须一一对应。
      fields: [_c0, _c3, _c4, _c5, _c6, _c7, _c8]

      # Nebula Graph 中 action 的属性名称，必须与 fields 里的列顺序一一对应。
      nebula.fields: [actionId, duration, feature0, feature1, feature2, feature3, label]

      # Exchange 1.1.0 添加了 csv.fields 参数：
      # 如果配置了 csv.fields，无论 CSV 文件是否有表头，
      # 均以这个参数指定的名称作为表头，
      # fields 的配置必须与 csv.fields 的配置保持一致。
      # csv.fields: [actionId, duration, feature0, feature1, feature2, feature3, label]

      # 边起点和边终点 VID 数据来源，
      # 如果不是 int 类型数据，则添加 policy 指定 VID 映射策略，建议设置为 "hash"。
      source: _c1
      target: {
        field: _c2
        policy: "hash"
      }

      # 标明数据源中数据分隔方式，默认为英文逗号（,）。
      separator: ","

      # 如果 CSV 文件中有表头，header 设置为 true。
      # 如果 CSV 文件中没有表头，header 设置为 false（默认）。
      header: false

      # 单次向 Nebula Graph 写入的最大边数据量。
      batch: 256

      # 设置 Spark 分区数量。
      partition: 32
      isImplicit: true
    }
  ]
  # 如果还有其他边，再添加其他边类型相关的设置。
}
```

### 步骤 4. （可选）检查配置文件是否正确

完成配置后，运行以下命令检查配置文件格式是否正确。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.1.0.jar -c /path/to/conf/csv_application.conf -D
```

### 步骤 5. 向 Nebula Graph 导入数据

运行以下命令将 CSV 文件数据导入到 Nebula Graph 中。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.1.0.jar -c /path/to/conf/csv_application.conf 
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
