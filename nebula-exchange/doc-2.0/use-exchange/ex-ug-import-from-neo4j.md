
# 导入 Neo4j 数据

您可以使用 Exchange 将 Neo4j 数据离线批量导入 Nebula Graph 数据库。

## 实现方法

Exchange 使用 **Neo4j Driver 4.0.1** 实现对 Neo4j 数据的读取。执行批量导出之前，您需要在配置文件中写入针对标签（label）和关系类型（Relationship Type）自动执行的 Cypher 语句，以及 Spark 分区数，提高数据导出性能。

Exchange 读取 Neo4j 数据时需要完成以下工作：

1. Exchange 中的 Reader 会将配置文件中 `exec` 部分的 Cypher `RETURN` 语句后面的语句替换为 `COUNT(*)`，并执行这个语句，从而获取数据总量，再根据 Spark 分区数量计算每个分区的起始偏移量和大小。
2. （可选）如果用户配置了 `check_point_path` 目录，Reader 会读取目录中的文件。如果当前处于续传的状态，Reader 会计算每个 Spark 分区应该有的偏移量和大小。
3. 在每个 Spark 分区里，Exchange 中的 Reader 会在 Cypher 语句后面添加不同的 `SKIP` 和 `LIMIT` 语句，调用 Neo4j Driver 并行执行，将数据分布到不同的 Spark 分区中。
4. Reader 最后将返回的数据处理成 DataFrame。

至此，Exchange 即完成了对 Neo4j 数据的导出。之后，数据被并行写入 Nebula Graph 数据库中。

整个过程如下图所示。

![Nebula Graph&reg; Exchange 从 Neo4j 数据库中导出数据再并行导入 Nebula Graph 数据库中](../figs/ex-ug-002.png "Nebula Graph&reg; Exchange 迁移 Neo4j 数据")

本文以一个示例说明如何使用 Exchange 将 Neo4j 的数据批量导入 Nebula Graph 数据库。

> **说明**：本文仅说明如何以客户端形式迁移数据。您也能通过 SST 文件将 Neo4j 的数据批量转入 Nebula Graph 数据库，具体操作基本相同，仅配置文件修改有差异，详细信息参考 [导入 SST 文件](ex-ug-import-sst.md)。

## 示例场景

假设您在 Neo4j 数据库里有一个图数据集，需要使用 Exchange 将数据迁移到 Nebula Graph 数据库中。

### 环境配置

以下为本示例中的环境配置信息：

- 服务器规格：
  - CPU：Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz
  - CPU 内核数：14
  - 内存：251 GB
- Spark：单机版，2.4.6 pre-build for Hadoop 2.7
- Neo4j：3.5.20 Community Edition
- Nebula Graph：V1.0.1，使用 Docker Compose 部署。详细信息，参考 [使用 Docker Compose 部署 Nebula Graph](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md)

### 数据集信息

Neo4j 的数据集信息如下：

- 两种标签（此处命名为 tagA 和 tagB），共计 100 万个节点（node）。
- 一种连接上述两类点的关系类型（Relationship Type，此处命名为 edgeAB），共计 1000 万条关系边（Relationship）。

上述三者分别拥有如下表所示的属性。

| 要素名称 | 属性 **idInt** | 属性 **idString** | 属性 **tboolean** | 属性 **tdouble** |
| :--- | :--- | :--- | :--- | --- |
| tagA | `int` | `string` | `bool` | `double` |
| tagB | `int` | `string` | `bool` | `double` |
| edgeAB | `int` | `string` | `bool` | `double` |

### 数据库信息

在本示例中 Neo4j 数据库和 Nebula Graph 数据库的信息如下：

- Neo4j 数据库：
  - 服务器地址及端口为：`bolt://127.0.0.1:7687`
  - 用户名：_neo4j_
  - 密码：_neo4j_

- Nebula Graph 数据库：
  - 服务器地址及端口为：`127.0.0.1:3699`
  - 用户名：本示例中 Nebula Graph 数据库没有启用身份认证，所以，用户名为 _user_
  - 密码：_password_

## 前提条件

开始迁移数据之前，您需要确保以下信息：

- 已经完成 Exchange 编译。详细信息，参考 [编译 Exchange](../ex-ug-compile.md)。

- 已经安装了 Spark。

- 在 Nebula Graph 中创建图数据模式所需的所有信息，包括标签和边类型的名称、属性等。

## 操作步骤

### 步骤 1. 在 Nebula Graph 中构建图数据模式（Schema）

根据示例场景，按以下步骤完成 Nebula Graph 构建图数据模式：

1. 确认图数据模式要素：Neo4j 中的标签和关系类型及其属性与 Nebula Graph 中的数据模式要素对应关系如下表所示。

    | Neo4j 要素 | Nebula Graph 要素 | 要素名称 |
    | :--- | :--- | :--- |
    | Node | Tag | tagA 和 tagB |
    | Relationship Type | Edge Type | edgAB |

2. 确认 Nebula Graph 需要的分区数量：全集群硬盘数量 * （2 至 10）。本示例中假设为 10。

3. 确认 Nebula Graph 需要的副本数量。本示例中假设为 1。

4. 在 Nebula Graph 里创建一个图空间 **test**，并创建一个图数据模式，如下所示。

    ```ngql
    -- 创建图空间，本示例中假设只需要一个副本
    nebula> CREATE SPACE test(partition_num=10, replica_factor=1);
    
    -- 选择图空间 test
    nebula> USE test;
    
    -- 创建标签 tagA
    nebula> CREATE TAG tagA(idInt int, idString string, tboolean bool, tdouble double);
    
    -- 创建标签 tagB
    nebula> CREATE TAG tagB(idInt int, idString string, tboolean bool, tdouble double);
    
    -- 创建边类型 edgeAB
    nebula> CREATE EDGE edgeAB(idInt int, idString string, tboolean bool, tdouble double);
    ```

关于 Nebula Graph 构图的更多信息，参考《Nebula Graph Database 手册》的 [快速开始](https://docs.nebula-graph.com.cn/manual-CN/1.overview/2.quick-start/1.get-started/ "点击前往 Nebula Graph 网站")。

### 步骤 2. 配置源数据

为了提高 Neo4j 数据的导出速度，在 Neo4j 数据库中为 tagA 和 tagB 的 `idInt` 属性创建索引。详细信息，参考 Neo4j 用户手册。

### 步骤 3. 修改配置文件

完成 Exchange 编译后，进入 `nebula-java/tools/exchange` 目录，您可以参考 `target/classes/server_application.conf` 修改配置文件。在本示例中，文件被重命名为 `neo4j_application.conf`。以下仅详细说明点和边数据的配置信息，本次示例中未使用的配置项已被注释，但是提供了配置说明。Spark 和 Nebula Graph 相关配置，参考 [Spark 参数](../parameter-reference/ex-ug-paras-spark.md)、[Nebula Graph 参数](../parameter-reference/ex-ug-paras-nebulagraph.md)。

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
      # 以下为 Nebula Graph 的 Graph 服务和 Meta 服务所在机器的 IP 地址及端口。
      # 如果有多个地址，格式为 "ip1:port","ip2:port","ip3:port"。
      # 不同地址之间以英文逗号 (,) 隔开。
      graph:["127.0.0.1:3699"]
      meta:["127.0.0.1:45500"]
    }
    # 填写的账号必须拥有 Nebula Graph 相应图空间的写数据权限。
    user: user
    pswd: password
    # 填写 Nebula Graph 中需要写入数据的图空间名称。
    space: test
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
      limit: 64M
      timeout: 1000
    }
  }

  # 处理点数据
  tags: [
    # 设置标签相关信息
    {
    name: tagA
    # 设置 Neo4j 数据库服务器地址，String 类型，格式必须为 "bolt://ip:port"。
    server: "bolt://127.0.0.1:7687"

    # Neo4j 数据库登录账号和密码。
    user: neo4j
    password: neo4j

    # 传输是否加密，默认值为 false，表示不加密；设置为 true 时，表示加密。
    encryption: false

    # 设置源数据所在 Neo4j 数据库的名称。如果您使用 Community Edition Neo4j，不支持这个参数。
    # database: graph.db

    type: {
        # 指定数据来源，设置为 neo4j。
        source: neo4j
        # 指定点数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
    }

    # 指定 Nebula Graph Schema 中标签对应的属性名称，以列表形式列出，
    # 与 tags.fields 列表中的属性名称一一对应，形成映射关系，
    # 多个属性名称之间以英文逗号（,）隔开。
    nebula.fields: [idInt, idString, tdouble, tboolean]

    # 指定源数据中与 Nebula Graph 标签对应的属性名称，
    # 以列表形式列出，多个属性名称之间以英文逗号（,）隔开，
    # 列出的属性名称必须与 exec 中列出的属性名称保持一致。
    fields       : [idInt, idString, tdouble, tboolean]
    
    # 将源数据中某个属性的值作为 Nebula Graph 点 VID 的来源，
    # 如果属性为 int 或者 long 类型，使用 vertex 设置 VID 列。
    vertex: idInt
    # 如果数据不是 int 类型，则添加 vertex.policy 指定 VID 映射策略，建议设置为 "hash"。
    # vertex {
    #     field: name
    #     policy: "hash"
    #}

    # Spark 的分区数量，默认值为 32。
    partition: 10

    # 单次写入 Nebula Graph 的点数据量，默认值为 256。
    batch: 2000

    # 设置保存导入进度信息的目录，用于断点续传，
    # 如果未设置，表示不启用断点续传。
    check_point_path: "file:///tmp/test"
    
    # 写入 Cypher 语句，从 Neo4j 数据库中检索打了某种标签的点的属性，并指定别名
    # Cypher 语句不能以英文分号（`;`）结尾。
    exec: "match (n:tagA) return n.idInt as idInt, n.idString as idString, n.tdouble as tdouble, n.tboolean as tboolean order by n.idInt"
    }
    # 如果有多个标签，则参考以上说明添加更多的标签相关信息。
  ]

  # 处理边数据
  edges: [
    {
      # 指定 Nebula Graph 中的边类型名称。
      name: edgeAB
      type: {
        # 指定数据来源，设置为 neo4j。
        source: neo4j

        # 指定边数据导入 Nebula Graph 的方式，
        # 可以设置为：client（以客户端形式导入）和 sst（以 SST 文件格式导入）。
        # 关于 SST 文件导入配置，参考文档：导入 SST 文件（https://
        # docs.nebula-graph.com.cn/nebula-exchange/
        # use-exchange/ex-ug-import-sst/）。
        sink: client
      }

      # 设置 Neo4j 数据库的信息，包括 IP 地址、端口、用户名和密码。
      server: "bolt://127.0.0.1:7687"
      user: neo4j
      password: neo4j

      # 写入 Cypher 语句，表示从 Neo4j 数据库中查询关系属性。
      # Cypher 语句不能以英文分号（`;`）结尾。
      exec: "match (a:tagA)-[r:edgeAB]->(b:tagB) return a.idInt, b.idInt, r.idInt as idInt, r.idString as idString, r.tdouble as tdouble, r.tboolean as tboolean order by id(r)"
      
      # 在 fields 里指定 Neo4j 中关系的属性名称，其对应的 value
      # 会作为 Nebula Graph 中 edgeAB 的属性（nebula.fields）值来源
      # fields 和 nebula.fields 里的配置必须一一对应
      # 如果需要指定多个列名称，用英文逗号（,）隔开
      fields: [r.idInt, r.idString, r.tdouble, r.tboolean]
      nebula.fields: [idInt, idString, tdboule, tboolean]      
      
      # 指定源数据中某个属性的值作为 Nebula Graph 边的起始点 VID。
      # 如果属性为 `int` 或者 `long` 类型，使用 source.field 设置起始点 VID 列
      # source.field: field_name
      # 如果不是上述类型的属性，则添加 source.policy 指定 VID 映射策略，建议设置为 "hash"。
      source: {
        field: a.idInt
        policy: "hash"
      }

      # 指定源数据中某个属性的值作为 Nebula Graph 边的终点 VID。
      # 如果属性为 `int` 或者 `long` 类型，使用 target.field 设置终点 VID 列
      # target.field: field_name
      # 如果不是上述类型的属性，则添加 target.policy 指定 VID 映射策略，建议设置为 "hash"。
      target: {
        field: b.idInt
        policy: "hash"
      }

      # 将源数据中某一列数据作为 Nebula Graph 中边的 Rank 值来源。
      ranking: idInt

      # Spark 的分区数量，默认值为 32。
      # 为减轻 Neo4j 的排序压力，将 partition 设置为 1
      partition: 1
   

   
      # 单次写入 Nebula Graph 的点数据量，默认值为 256
      batch: 1000
      
      # 设置保存导入进度信息的目录，用于断点续传。如果未设置，表示不启用断点续传。
      check_point_path: /tmp/test
    }
  ]
}
```

#### exec 配置说明

在配置源数据的 `tags.exec` 或者 `edges.exec` 参数时，需要写入 Cypher 查询语句。为了保证每次查询结果排序一致，并且为了防止在导入时丢失数据，强烈建议您在 Cypher 查询语句中加入 `ORDER BY` 子句，同时，为了提高数据导入效率，最好选取有索引的属性作为排序的属性。如果没有索引，您也可以观察默认的排序，选择合适的属性用于排序，以提高效率。如果默认的排序找不到规律，您可以根据点或关系的 ID 进行排序，并且将 `partition` 设置为一个尽量小的值，减轻 Neo4j 的排序压力。

> **说明**：使用 `ORDER BY` 子句会延长数据导入的时间。

另外，Exchange 需要在不同 Spark 分区执行不同 `SKIP` 和 `LIMIT` 的 Cypher 语句，所以，在 `tags.exec` 和 `edges.exec` 对应的 Cypher 语句中不能含有 `SKIP` 和 `LIMIT` 子句。

#### tags.vertex 或 edges.vertex 配置说明

Nebula Graph 在创建点和边时会将 ID 作为唯一主键，如果主键已存在则会覆盖该主键中的数据。所以，假如将某个 Neo4j 属性值作为 Nebula Graph 的 ID，而这个属性值在 Neo4j 中是有重复的，就会导致“重复 ID”，它们对应的数据有且只有一条会存入 Nebula Graph 中，其它的则会被覆盖掉。由于数据导入过程是并发地往 Nebula Graph 中写数据，最终保存的数据并不能保证是 Neo4j 中最新的数据。

#### check_point_path 配置说明

如果启用了断点续传功能，为避免数据丢失，在断点和续传之间，数据库不应该改变状态，例如不能添加数据或删除数据，同时，不能更改 `partition` 数量配置。

### 步骤 4. （可选）检查配置文件是否正确

完成配置后，运行以下命令检查配置文件格式是否正确。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local[10]" --class com.vesoft.nebula.tools.importer.Exchange target/exchange-1.x.y.jar -c /path/to/conf/neo4j_application.conf -D
```

### 步骤 5. 向 Nebula Graph 导入数据

运行以下命令使用 Exchange 将 Neo4j 的数据迁移到 Nebula Graph 中。关于参数的说明，参考 [导入命令参数](../parameter-reference/ex-ug-para-import-command.md)。

```bash
$SPARK_HOME/bin/spark-submit --master "local[10]" --class com.vesoft.nebula.tools.importer.Exchange target/exchange-1.x.y.jar -c /path/to/conf/neo4j_application.conf
```

> **说明**：JAR 文件版本号以您实际编译得到的 JAR 文件名称为准。

### 步骤 6. （可选）验证数据

您可以在 Nebula Graph 客户端（例如 Nebula Graph Studio）里执行语句，确认数据是否已导入，例如：

```ngql
GO FROM <tagA_VID> OVER edgeAB;
```

如果返回边终点（`edgeAB._dst`）即表明数据已导入。

您也可以使用 db_dump 工具统计数据是否已经全部导入。详细的使用信息参考 [Dump Tool](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/5.storage-service-administration/data-export/dump-tool/)。

### 步骤 7. （可选）在 Nebula Graph 中重构索引

Exchange 导入数据时，并不会导入 Neo4j 数据库中的索引，所以，导入数据后，您可以在 Nebula Graph 中重新创建并重构索引。详细信息，参考[《Nebula Graph Database 手册》](https://docs.nebula-graph.com.cn/manual-CN/2.query-language/4.statement-syntax/1.data-definition-statements/ "点击前往 Nebula Graph 网站")。

## 性能说明

以下为单次测试数据，仅供参考。测试性能如下：

- 导入 100 万个点，耗时 9 s
- 导入 1,000 万条边，耗时 43 s
- 总耗时：52 s

## 附录：Neo4j 3.5 Community 和 Nebula Graph 1.0.1 对比

Neo4j 和 Nebula Graph 在系统架构、数据模型和访问方式上都有一些差异，部分差异可能会对您使用 Exchange 迁移数据产生影响。下表列出了常见的异同。

<table>
 <tr>
  <th colspan=2>对比项</th>
  <th>Neo4j 3.5 Conmunity</th>
  <th>Nebula Graph 1.0.1</th>
  <th>对使用 Exchange 的影响</th>
 </tr>
 <tr>
  <td rowspan=5>系统架构</td>
  <td>分布式</td>
  <td>仅 Enterprise 支持</td>
  <td>支持</td>
  <td>无</td>
 </tr>
 <tr>
  <td>数据分片</td>
  <td>不支持</td>
  <td>支持</td>
  <td>无</td>
 </tr>
 <tr>
  <td>开源协议</td>
  <td>AGPL</td>
  <td>Apache 2.0</td>
  <td>无</td>
 </tr>
 <tr>
  <td>开发语言</td>
  <td>Java</td>
  <td>C++</td>
  <td>无</td>
 </tr>
 <tr>
  <td>高可用</td>
  <td>不支持</td>
  <td>支持</td>
  <td>无</td>
 </tr>
 <tr>
  <td rowspan=7>数据模型</td>
  <td>属性图</td>
  <td>是</td>
  <td>是</td>
  <td>无</td>
 </tr>
 <tr>
  <td>Schema</td>
  <td>Schema optional 或者 Schema free</td>
  <td>强 Schema</td>
  <td>
   <ul>
    <li>必须在 Nebula Graph 中事先创建Schema</li>
    <li>Neo4j 中的 Schema 必须与 Nebula Graph 的保持一致</li>
   </ul>
  </td>
 </tr>
 <tr>
  <td>点类型/边类型</td>
  <td>Label（可以没有 Label，Label 不决定属性 Schema）</td>
  <td>Tag/EdgeType（必须至少有一个 Tag，并且与 Schema 对应）</td>
  <td>无</td>
  </tr>  
 <tr>
  <td>点 ID/唯一主键</td>
  <td>点允许无主键（会有多条重复记录）。由内置 <code>id()</code> 标识，或者由约束来保证主键唯一。</td>
  <td>点必须有唯一标识符，称为 VID。VID 由应用程序生成。</td>
  <td>主键相同的重复记录只保留最新的一份。</td>
 </tr>
 <tr>
  <td>属性索引</td>
  <td>支持</td>
  <td>支持</td>
  <td>无法导入索引，必须在导入后再重新创建。</td>
 </tr>
 <tr>
  <td>约束（Constrains）</td>
  <td>支持</td>
  <td>不支持</td>
  <td>不会导入约束。</td>
 </tr>
 <tr>
  <td>事务</td>
  <td>支持</td>
  <td>不支持</td>
  <td>无</td>
 </tr>
 <tr>
  <td rowspan=6>查询语句示例</td>
  <td>列出所有 labels 或 tags</td>
  <td><code>MATCH (n) RETURN distinct labels(n);<br>call db.labels();</code>
  <td><code>SHOW TAGS;</code></td>
  <td>无</td>
 </tr>
 <tr>
  <td>插入指定类型的点</td>
  <td><code>CREATE (:Person {age: 16})</code></td>
  <td><code>INSERT VERTEX &lt;tag_name&gt; (prop_name_list) VALUES &lt;vid&gt;:(prop_value_list)</code></td>
  <td>无</td>
 </tr>
 <tr>
  <td>更新点属性</td>
  <td><code>SET n.name = V</code></td>
  <td><code>UPDATE VERTEX &lt;vid&gt; SET &lt;update_columns&gt;</code></td>
  <td>无</td>
 </tr>
 <tr>
  <td>查询指定点的属性</td>
  <td><code>MATCH (n)<br>WHERE ID(n) = vid<br>RETURN properties(n)</code></td>
  <td><code>FETCH PROP ON &lt;tag_name&gt; &lt;vid&gt;</code></td>
  <td>无</td>
 </tr>
 <tr>
  <td>查询指定点的某一类关系</td>
  <td><code>MATCH (n)-[r:edge_type]-&gt;() WHERE ID(n) = vid</code></td>
  <td><code>GO FROM &lt;vid&gt; OVER &lt;edge_type&gt;</code></td>
  <td>无</td>
 </tr>
 <tr>
  <td>两点路径</td>
  <td><code>MATCH p =(a)-[]-&gt;(b)<br>WHERE ID(a) = a_vid AND ID(b) = b_vid<br>RETURN p</code></td>
  <td><code>FIND ALL PATH FROM &lt;a_vid&gt; TO &lt;b_vid&gt; OVER *</code></td>
  <td>无</td>
 </tr>
</table>
