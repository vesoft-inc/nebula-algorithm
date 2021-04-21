# Nebula Exchange 2.0
 [中文版](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-exchange/README-CN.md)
 
Nebula Exchange (Exchange for short) is an Apache Spark application. It is used to migrate cluster data in bulk from Spark to Nebula Graph in a distributed environment. It supports migration of batch data and streaming data in various formats.

Exchange 2.0 only supports Nebula Graph 2.0 . If you want to import data for Nebula Graph v1.x，please use [Nebula Exchange v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/exchange).

## How to get

1. Package Exchange 2.0。

    ```bash
    $ git clone -b v2.0.0 https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-exchange
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    After the packaging, you can see the newly generated nebula-exchange-2.0.0.jar under the nebula-spark-utils/nebula-exchange/target/ directory.
2. Download from Maven repository
   
   https://repo1.maven.org/maven2/com/vesoft/nebula-exchange/2.0.0/
## How to use

Import command:
```
$SPARK_HOME/bin/spark-submit --class com.vesoft.nebula.exchange.Exchange --master local nebula-exchange-2.0.0.jar -c /path/to/application.conf
```

For more details about Exchange, please refer to [Exchange 1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/exchange) .


## New Features

1. Supports importing vertex data with String and Integer type IDs.
2. Supports importing data of the Null, Date, DateTime, and Time types(DateTime uses UTC, not local time).
3. Supports importing data from other Hive sources besides Hive on Spark.
4. Supports recording and retrying the INSERT statement after failures during data import.

Refer to [application.conf](https://github.com/vesoft-inc/nebula-spark-utils/tree/main/nebula-exchange/src/main/resources) as an example to edit the configuration file.
