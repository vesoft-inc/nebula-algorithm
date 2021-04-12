# Spark related parameters

To import data, you must set parameters for Spark. This table lists some generally-used parameters. For more Spark-related parameters, see [Apache Spark documentation](https://spark.apache.org/docs/latest/configuration.html#application-properties "Click to go go Spark documenation"). For more information, see the [examples](../use-exchange/ex-ug-import-steps.md).

| Parameters | Default | Data type | Required? | Description |
| :--- | :--- | :--- | :--- | :--- |
| `spark.app.name` | Nebula Exchange 2.0 | `string` | No | Specifies the name of the Spark Driver Program.|
| `spark.driver.cores` | 1 | `int` | No | Specifies the number of cores to use for the driver process, only in cluster mode. |
| `spark.driver.maxResultSize` | 1G | `string` | No | Specifies the limit of the total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. |
| `spark.cores.max` | None | `int` | No | When the driver program runs on a standalone deployed cluster or a Mesos cluster in "coarse-grained" sharing mode, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine). If not set, the default will be `spark.deploy.defaultCores` on the standalone cluster manager of Spark, or infinite (all available cores) on Mesos. |
