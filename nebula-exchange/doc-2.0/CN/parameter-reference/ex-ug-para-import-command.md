# 导入命令参数

完成配置文件修改后，可以运行以下命令将指定来源的数据导入Nebula Graph数据库。

```bash
<spark_install_path>/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange <nebula-exchange-2.x.y.jar_path> -c <csv_application.conf_path> 
```

> **说明**：JAR文件版本号以您实际编译得到的JAR文件名称为准。

下表列出了命令的相关参数。

| 参数 | 是否必需 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `--class`  | 是 | 无 | 指定驱动的主类。 |
| `--master`  | 是 | 无 | 指定Spark集群中master进程的URL。详情请参见[master-urls](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls "点击前往 Apache Spark 文档")。 |
| `-c`  / `--config`  | 是 | 无 | 指定配置文件的路径。 |
| `-h`  / `--hive`  | 否 | `false` | 添加这个参数表示支持从Hive中导入数据。 |
| `-D`  / `--dry`  | 否 | `false` | 添加这个参数表示检查配置文件的格式是否符合要求，但不会校验`tags`和`edges`的配置项是否正确。正式导入数据时不能添加这个参数。 |
