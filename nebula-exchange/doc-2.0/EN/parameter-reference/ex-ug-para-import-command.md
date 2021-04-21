# Import command parameters

When the configuration file is ready, replace `master-node-url` and `exchange-2.x.y.jar` in this command and run it to import the data from the specified source into Nebula Graph.

```bash
$SPARK_HOME/bin/spark-submit --master "master-node-url" --class com.vesoft.nebula.exchange.Exchange target/exchange-2.x.y.jar -c /path/to/conf/application.conf
```

This table lists all the parameters in the preceding command.

| Parameters | Required? | Default | Description |
| :--- | :--- | :--- | :--- |
| `--master`  | Yes | None | Specifies the URL of the Master node of the specified Spark cluster. For more information, see [master-urls in Spark Documentation](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls "Click to go to Apache Spark Documentation")。 |
| `--class`  | Yes | None | Specifies the entry point of Exchange. |
| `-c`  / `--config`  | Yes | None | Specifies the path of the Exchange configuration file. |
| `-h`  / `--hive`  | No | `false` | If you want to import data from Hive, add this parameter. |
| `-D`  / `--dry`  | No | `false` | Before data import, add this parameter to do a check of the format of the configuration file, but not the configuration of `tags` and `edges`. Do not use this parameter when you import data. |
