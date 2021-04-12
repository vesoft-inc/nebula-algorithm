# 导入数据步骤

您可以按本文描述的步骤使用 Exchange 将指定来源的数据导入到 Nebula Graph 中。

## 前提条件

开始迁移数据之前，您需要确保以下信息：

- 已经安装部署了 Nebula Graph 并获取查询引擎所在服务器的 IP 地址、用户名和密码。

- 已经完成 Exchange 编译。详细信息，参考 [编译 Nebula Exchange](../ex-ug-compile.md)。

- 已经安装 Spark。

- 在 Nebula Graph 中创建图数据模式需要的所有信息，包括标签和边类型的名称、属性等。

## 操作步骤

按以下步骤将不同来源的数据导入 Nebula Graph 数据库：

1. 在 Nebula Graph 中构图，包括创建图空间、创建图数据模式（Schema）。

2. （可选）处理源数据。例如，在导入 Neo4j 时，为提高导出速度，在 Neo4j 数据库中为指定的标签属性创建索引。

3. 分别修改 Spark、Nebula Graph 以及点和边数据的配置文件。
   > **说明**：完成 Exchange 编译后，进入 `nebula-java/tools/exchange` 目录，您可以参考 `target/classes/server_application.conf`、`target/classes/application.conf` 和 `target/classes/stream_application.conf` 文件修改配置文件。

4. （可选）检查配置文件是否正确。

5. 向 Nebula Graph 导入数据。

6. 在 Nebula Graph 中验证数据是否已经完整导入。

7. （可选）在 Nebula Graph 中重构索引。

关于详细操作步骤，根据数据来源不同，您可以参考相应的操作示例：

- [导入 Neo4j 数据](ex-ug-import-from-neo4j.md)
- [导入 HIVE 数据](ex-ug-import-hive.md)
- [导入 CSV 文件数据](ex-ug-import-from-csv.md)
- [导入 JSON 文件数据](ex-ug-import-json.md)
- [导入 SST 文件数据](ex-ug-import-sst.md)
