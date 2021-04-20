# FAQ

**What version of Nebula Graph does Exchange v2.x support?**

Read [Limitations](ex-ug-limitations.md) to get the latest information about supported Nebula Graph versions.

**What are the differences between Exchange v1.x and Exchange v2.x?**

Compared with Exchange v1.x, Exchange v2.x has these new features:

- Importing vertex data with String type IDs.
- Importing data of the Null, Date, DateTime, and Time types.
- Importing data from other Hive sources besides Hive on Spark.
- Recording and retrying the INSERT statement after failures during data import.

For more information, see [Exchange README](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-exchange).

**What is the difference between Exchange and Spark Writer?**

Both are Spark applications, and Exchange is based on Spark Writer. Both of them are designed for the migration of data into a Nebula Graph cluster in a distributed environment, but the later maintenance work will focus on Exchange. Compared with Spark Writer, Exchange has the following improvements:

- Supporting more data sources, such as MySQL, Neo4j, Hive, HBase, Kafka, and Pulsar.

- Some problems with Spark Writer were fixed. For example, by default Spark reads source data from HDFS as strings, which is probably different from your graph schema defined in Nebula Graph. Exchange supports automatically matching and converting data types. With it, when a non-string data type is defined in Nebula Graph, Exchange converts the strings into data of the required data type.

<!---
TODO:doc

## 编译问题

### 部分非central仓库的包下载失败，报错`Could not resolve dependencies for project xxx`

请检查Maven安装目录下`libexec/conf/settings.xml`文件的`mirror`部分：

```text
<mirror>
    <id>alimaven</id>
    <mirrorOf>central</mirrorOf>
    <name>aliyun maven</name>
    <url>http://maven.aliyun.com/nexus/content/repositories/central/</url>
</mirror>
```

检查`mirrorOf`的值是否配置为`*`，如果为`*`，请修改为`central`或`*,!SparkPackagesRepo,!bintray-streamnative-maven`。

**原因**：Exchange的`pom.xml`中有两个依赖包不在Maven的central仓库中，`pom.xml`配置了这两个依赖所在的仓库地址。如果您的maven中配置的镜像地址对应的`mirrorOf`值为`*`，那么所有依赖都会在central仓库下载，导致下载失败。

## 执行问题

### 报错`method name xxx not found`

一般是端口配置错误，需检查Meta服务、Graph服务、Storage服务的端口配置。

### 报NoSuchMethod、MethodNotFound错误（`Exception in thread "main" java.lang.NoSuchMethodError`等）

绝大多数是因为JAR包冲突和版本冲突导致的报错，请检查报错服务的版本，与Exchange中使用的版本进行对比，检查是否一致，尤其是Spark版本、Scala版本、Hive版本。

### Exchange导入Hive数据时报错`Exception in thread "main" org.apache.spark.sql.AnalysisException: Table or view not found`

检查提交exchange任务的命令中是否遗漏参数`-h`，检查table和database是否正确，在spark-sql中执行用户配置的exec语句，验证exec语句的正确性。

### 运行时报错`com.facebook.thrift.protocol.TProtocolException: Expected protocol id xxx`

请检查Nebula Graph服务端口配置是否正确。

- 如果是源码、RPM或DEB安装，请配置各个服务的配置文件中`--port`对应的端口号。

- 如果是docker安装，请配置docker映射出来的端口号，查看方式如下：

    在`nebula-docker-compose`目录下执行`docker-compose ps`，例如：

    ```bash
    $ docker-compose ps
                  Name                             Command                  State                                                         Ports
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    nebula-docker-compose_graphd_1      /usr/local/nebula/bin/nebu ...   Up (healthy)   0.0.0.0:33205->19669/tcp, 0.0.0.0:33204->19670/tcp, 0.0.0.0:9669->9669/tcp
    nebula-docker-compose_metad0_1      ./bin/nebula-metad --flagf ...   Up (healthy)   0.0.0.0:33165->19559/tcp, 0.0.0.0:33162->19560/tcp, 0.0.0.0:33167->9559/tcp, 9560/tcp
    nebula-docker-compose_metad1_1      ./bin/nebula-metad --flagf ...   Up (healthy)   0.0.0.0:33166->19559/tcp, 0.0.0.0:33163->19560/tcp, 0.0.0.0:33168->9559/tcp, 9560/tcp
    nebula-docker-compose_metad2_1      ./bin/nebula-metad --flagf ...   Up (healthy)   0.0.0.0:33161->19559/tcp, 0.0.0.0:33160->19560/tcp, 0.0.0.0:33164->9559/tcp, 9560/tcp
    nebula-docker-compose_storaged0_1   ./bin/nebula-storaged --fl ...   Up (healthy)   0.0.0.0:33180->19779/tcp, 0.0.0.0:33178->19780/tcp, 9777/tcp, 9778/tcp, 0.0.0.0:33183->9779/tcp, 9780/tcp
    nebula-docker-compose_storaged1_1   ./bin/nebula-storaged --fl ...   Up (healthy)   0.0.0.0:33175->19779/tcp, 0.0.0.0:33172->19780/tcp, 9777/tcp, 9778/tcp, 0.0.0.0:33177->9779/tcp, 9780/tcp
    nebula-docker-compose_storaged2_1   ./bin/nebula-storaged --fl ...   Up (healthy)   0.0.0.0:33184->19779/tcp, 0.0.0.0:33181->19780/tcp, 9777/tcp, 9778/tcp, 0.0.0.0:33185->9779/tcp, 9780/tcp
    ```

    查看`Ports`列，查找docker映射的端口号，例如：

    - Graph服务可用的端口号是9669。

    - Meta服务可用的端口号有33167、33168、33164。

    - Storage服务可用的端口号有33183、33177、33185。

## 配置问题

### 哪些配置项影响导入性能？

- batch：每次发送给Nebula Graph服务的nGQL语句中包含的数据条数。

- partition：Spark数据的分区数，表示数据导入的并发数。

- nebula.rate：向Nebula Graph发送请求前先去令牌桶获取令牌。

    - limit：表示令牌桶的大小。

    - timeout：表示获取令牌的超时时间。

根据机器性能可适当调整这四项参数的值。如果在导入过程中，Storage服务的leader变更，可以适当调小这四项参数的值，降低导入速度。

## 其他问题

### Exchange支持哪些版本的Nebula Graph？

请参见Exchange的[使用限制](about-exchange/ex-ug-limitations.md)。

### Exchange与Spark Writer有什么关系？

Exchange是在Spark Writer基础上开发的Spark应用程序，二者均适用于在分布式环境中将集群的数据批量迁移到Nebula Graph中，但是后期的维护工作将集中在 Exchange上。与Spark Writer相比，Exchange有以下改进：

- 支持更丰富的数据源，如MySQL、Neo4j、Hive、HBase、Kafka、Pulsar等。

- 修复了Spark Writer的部分问题。例如Spark读取HDFS里的数据时，默认读取到的源数据均为String类型，可能与Nebula Graph定义的Schema不同，所以Exchange增加了数据类型的自动匹配和类型转换，当Nebula Graph定义的Schema中数据类型为非String类型（如double）时，Exchange会将String类型的源数据转换为对应的类型（如double）。

--->
