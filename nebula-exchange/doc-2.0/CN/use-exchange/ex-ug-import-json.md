# 导入 JSON 文件数据

本文以一个示例说明如何使用 Exchange 将存储在 HDFS 上的 JSON 文件数据导入 Nebula Graph。

## 数据集

本示例所用 JSON 文件（test.json）结构为：`{"source":int, "target":int, "likeness":double}`，表示 `source` 与 `target` 之间一种 `like` 关系。共计 21,645 条数据。

以下为部分示例数据：

```json
{"source":53802643,"target":87847387,"likeness":0.34}
{"source":29509860,"target":57501950,"likeness":0.40}
{"source":97319348,"target":50240344,"likeness":0.77}
{"source":94295709,"target":8189720,"likeness":0.82}
{"source":78707720,"target":53874070,"likeness":0.98}
{"source":23399562,"target":20136097,"likeness":0.47}
```

## 环境配置

本文示例在 MacOS 下完成，以下是相关的环境配置信息：

- 硬件规格：
  - CPU：1.7 GHz Quad-Core Intel Core i7
  - 内存：16 GB

- Spark：2.3.0，单机版

- Hadoop：2.9.2，伪分布式部署

- Nebula Graph：V1.1.0，使用 Docker Compose 部署。详细信息，参考 [使用 Docker Compose 部署 Nebula Graph](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)

## 前提条件

开始迁移数据之前，您需要确认以下信息：

- 已经完成 Exchange 编译。详细信息，参考 [编译 Exchange](../ex-ug-compile.md)。本示例中使用 Exchange v1.0.1。

- 已经安装 Spark。

- 已经安装并开启 Hadoop 服务。

- 已经部署并启动 Nebula Graph，并获取：
  - Graph 服务、Meta 服务所在机器的 IP 地址和端口信息。
  - Nebula Graph 数据库的拥有写权限的用户名及其密码。

- 在 Nebula Graph 中创建图数据模式（Schema）所需的所有信息，包括标签和边类型的名称、属性等。

## 操作步骤

### 步骤 1. 在 Nebula Graph 中创建 Schema

分析 JSON 文件中的数据，按以下步骤在 Nebula Graph 中创建 Schema：

1. 确认 Schema 要素：Nebula Graph 中的 Schema 要素如下表所示。

    | 要素  | 名称 | 属性 |
    | :--- | :--- | :--- |
    | 标签（Tag） | `source` | `srcId int` |
    | 标签（Tag） | `target` | `dstId int` |
    | 边类型（Edge Type） | `like` | `likeness double` |

2. 在 Nebula Graph 里创建一个图空间 **json**，并创建一个 Schema，如下所示。

    ```ngql
    -- 创建图空间
    CREATE SPACE json (partition_num=10, replica_factor=1);
    
    -- 选择图空间 json
    USE json;
    
    -- 创建标签 source
    CREATE TAG source (srcId int);
    
    -- 创建标签 target
    CREATE TAG target (dstId int);
    
    -- 创建边类型 like
    CREATE EDGE like (likeness double);
    ```

关于 Nebula Graph 构图的更多信息，参考《Nebula Graph Database 手册》的 [快速开始](https://docs.nebula-graph.com.cn/manual-CN/1.overview/2.quick-start/1.get-started/ "点击前往 Nebula Graph 网站")。

### 步骤 2. 处理 JSON 文件

分别创建点和边数据 JSON 文件。同时，JSON 文件必须存储在 HDFS 里，并获取文件存储路径。

> **说明**：本示例中仅使用一个 JSON 文件同时写入点和边数据，其中，表示 source 和 target 的部分点数据是重复的，所以，在写入数据时，这些点会被重复写入。向 Nebula Graph 插入点或边时，允许重复插入，但是最后读取时以最后一次写入的数据为准，所以，并不影响使用。在实际使用时，最好分别创建点和边数据文件，提高数据写入速度。

### 步骤 3. 修改配置文件

完成 Exchange 编译后，进入 `nebula-java/tools/exchange` 目录，根据 `target/classes/application.conf` 文件修改 果断 数据源相关的配置文件。在本示例中，文件被重命名为 `json_application.conf`。以下配置文件中提供了 JSON 源数据所有配置项。本次示例中未使用的配置项已被注释，但是提供了配置说明。Spark 和 Nebula Graph 相关配置，参考 [Spark 参数](../parameter-reference/ex-ug-paras-spark.md)和 [Nebula Graph 参数](../parameter-reference/ex-ug-paras-nebulagraph.md)。

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
    space: json

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
    # 设置标签 source 相关信息
    {
      # 设置为 Nebula Graph 中对应的标签名称
      name: source
      type: {
        # 指定数据源文件格式，设置为 json。
        source: json

        # 指定标签数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件。
        sink: client
      }

      # JSON 文件所在的 HDFS 路径，String 类型，必须以 hdfs:// 开头。
      path: "hdfs://namenode_ip:port/path/to/test.json"

      # 在 fields 里指定 JSON 文件中 key 名称，其对应的 value
      # 会作为 Nebula Graph 中指定属性 srcId 的数据源
      # 如果需要指定多个值，用英文逗号（,）隔开
      fields: ["source"]
      nebula.fields: ["srcId"]

      # 将 JSON 文件中某个 key 对应的值作为 Nebula Graph 中点 VID 的来源
      # 如果 VID 源数据不是 int 类型，则使用以下内容来代替 vertex 的设置，在其中指定 VID 映射策略，建议设置为 "hash"。
      # vertex: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      vertex: source

      batch: 256
      partition: 32

      # isImplicit 设置说明，详见 https://github.com/vesoft-inc/
      # nebula-java/blob/v1.0/tools/exchange/src/main/resources/
      # application.conf
      isImplicit: true
    }
    # 设置标签 target 相关信息
    {
      name: target
      type: {
        source: json
        sink: client
      }
      path: "hdfs://namenode_ip:port/path/to/test.json"
      fields: ["target"]
      nebula.fields: ["dstId"]
      vertex: "target"
      batch: 256
      partition: 32
      isImplicit: true
    }
  # 如果还有其他标签，参考以上配置添加
  ]

  # 处理边数据
  edges: [
    # 设置边类型 like 相关信息
    {
      # Nebula Graph 中对应的边类型名称。
      name: like
      type: {
        # 指定数据源文件格式，设置为 json。
        source: json

        # 指定边数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }

      # 指定 JSON 文件所在的 HDFS 路径，String 类型，必须以 hdfs:// 开头。
      path: "hdfs://namenode_ip:port/path/to/test.json"

      # 在 fields 里指定 JSON 文件中 key 名称，其对应的 value
      # 会作为 Nebula Graph 中指定属性 likeness 的数据源
      # 如果需要指定多个值，用英文逗号（,）隔开
      fields: ["likeness"]
      nebula.fields: ["likeness"]

      # 将 JSON 文件中某两个 key 对应的值作为 Nebula Graph 中边起点和边终点 VID 的来源
      # 如果 VID 源数据不是 int 类型，则使用以下内容来代替 source 
      # 和/或 target 的设置，在其中指定 VID 映射策略，建议设置为 "hash"。
      # source: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      # target: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      source: "source"
      target: "target"

      batch: 256
      partition: 32
      isImplicit: true
    }
    # 如果还有其他边类型，参考以上配置添加
  ]
}
```

### 步骤 4. （可选）检查配置文件是否正确

完成配置后，运行以下命令检查配置文件格式是否正确。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.0.1.jar -c /path/to/conf/json_application.conf -D
```

### 步骤 5. 向 Nebula Graph 导入数据

运行以下命令将 JSON 文件数据导入 Nebula Graph 中。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.tools.importer.Exchange /path/to/exchange-1.0.1.jar -c /path/to/conf/json_application.conf
```

### 步骤 6. （可选）验证数据

您可以在 Nebula Graph 客户端（例如 Nebula Graph Studio）里执行语句，确认数据是否已导入，例如：

```ngql
GO FROM 53802643 OVER like;
```

如果返回边终点（`like._dst`）即表明数据已导入。

您也可以使用 db_dump 工具统计数据是否已经全部导入。详细的使用信息参考 [Dump Tool](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/5.storage-service-administration/data-export/dump-tool/)。

### 步骤 7. （可选）在 Nebula Graph 中重构索引

导入数据后，您可以在 Nebula Graph 中重新创建并重构索引。详细信息，参考[《Nebula Graph Database 手册》](https://docs.nebula-graph.com.cn/manual-CN/2.query-language/4.statement-syntax/1.data-definition-statements/ "点击前往 Nebula Graph 网站")。
