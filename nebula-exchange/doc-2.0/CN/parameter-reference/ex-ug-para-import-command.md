# 导入命令参数

完成配置文件修改后，运行以下命令将指定来源的数据导入 Nebula Graph 数据库。

```bash
$SPARK_HOME/bin/spark-submit  --class com.vesoft.nebula.tools.importer.Exchange --master "local[10]" target/exchange-1.x.y.jar -c /path/to/conf/application.conf
```

> **说明**：JAR 文件版本号以您实际编译得到的 JAR 文件名称为准。

下表列出了命令的相关参数。

| 参数 | 是否必需 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `--class`  | 是 | 无 | 指定 Driver 主类。 |
| `--master`  | 是 | 无 | 指定 Spark 集群中 Master 进程的 URL。详细信息参考 [master-urls](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls "点击前往 Apache Spark 文档")。 |
| `-c`  / `--config`  | 是 | 无 | 指定配置文件的路径。 |
| `-h`  / `--hive`  | 否 | `false` | 添加这个参数表示支持从 HIVE 中导入数据。 |
| `-D`  / `--dry`  | 否 | `false` | 添加这个参数表示检查配置文件的格式是否符合要求，但不会校验 `tags` 和 `edges` 的配置项是否正确。正式导入数据时不能添加这个参数。 |
