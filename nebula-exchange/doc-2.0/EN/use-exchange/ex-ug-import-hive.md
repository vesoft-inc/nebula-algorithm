# Import data from Hive

This article uses an example to show how to use Exchange to import data from Hive into Nebula Graph.

## Dataset

In this article, the [Social Network: MOOC User Action Dataset](https://snap.stanford.edu/data/act-mooc.html "Click to go to Stanford Network Analysis Platform") provided by Stanford Network Analysis Platform (SNAP) and 97 unique course names obtained from the public network are used as the sample dataset. The dataset contains:

- Two vertex types (`user` and `course`), 7,144 vertices in total.
- One edge type (`action`), 411,749 edges in total.

You can download the example dataset from the [nebula-web-docker](https://github.com/vesoft-inc/nebula-web-docker/tree/master/example/mooc-actions "Click to go to GitHub") repository.

In this example, the dataset is stored in a database named `mooc` in Hive, and the information of all vertices and edges is stored in the `users`, `courses`, and `actions` tables. Here are the structures of all the tables.

```sql
scala> sql("describe mooc.users").show
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|  userid|   string|   null|
+--------+---------+-------+

scala> sql("describe mooc.courses").show
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|  courseid|   bigint|   null|
|coursename|   string|   null|
+----------+---------+-------+

scala> sql("describe mooc.actions").show
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|actionid|   bigint|   null|
|   srcid|   string|   null|
|   dstid|   string|   null|
|duration|   double|   null|
|feature0|   double|   null|
|feature1|   double|   null|
|feature2|   double|   null|
|feature3|   double|   null|
|   label|  boolean|   null|
+--------+---------+-------+
```

> **NOTE**: `bigint` in Hive equals to `int` in Nebula Graph.

## Environment

The practice is done in macOS. Here is the environment information:

- Hardware specifications:
  - CPU: 1.7 GHz Quad-Core Intel Core i7
  - Memory: 16 GB

- Spark 2.4.7, deployed in the Standalone mode

- Hadoop 2.9.2, deployed in the Pseudo-Distributed mode

- Hive 2.3.7, with MySQL 8.0.22

- Nebula Graph v2-nightly, deployed with Docker Compose. For more information, see [Deploy Nebula Graph with Docker Compose](https://github.com/vesoft-inc/nebula-docker-compose "Click to go to GitHub").

## Prerequisites

To import data from Hive with Exchange v2.x, do a check of these:

- Exchange v2.x is compiled. For more information, see [Compile Exchange v2.x](../ex-ug-compile.md). Exchange 2.0.0 is used in this example.

- Spark is installed.

- Hadoop is installed and started and the hive metastore database (MySQL is used in this example) is started.

- Nebula Graph is deployed and started. Get the information:
  - IP addresses and ports of the Graph Service and the Meta Service.
  - A Nebula Graph account with the privilege of writing data and its password.

- Get the necessary information for schema creation in Nebula Graph, including tags and edge types.

## Procedure

### Step 1. Create a schema in Nebula Graph

Follow these steps to create a schema in Nebula Graph:

1. Confirm the essential elements of the schema.

    | Elements  | Names | Properties |
    | :--- | :--- | :--- |
    | Tag | `user` | `userId string` |
    | Tag | `course` | `courseId int, courseName string` |
    | Edge Type | `action` | `actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double` |

2. In Nebula Graph, create a graph space named **hive** and create a schema.

    ```ngql
    -- Create a graph space named hive
    CREATE SPACE hive(partition_num=10, replica_factor=1, vid_type=fixed_string(100));
    
    -- Choose the hive graph space
    USE hive;
    
    -- Create the user tag
    CREATE TAG user(userId string);
    
    -- Create the course tag
    CREATE TAG course(courseId int, courseName string);
    
    -- Create the action edge type
    CREATE EDGE action (actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double);
    ```

For more information, see [Quick Start of Nebula Graph](../../2.quick-start/4.nebula-graph-crud.md).

### Step 2. Verify the Hive SQL statements

When spark-shell starts, run these statements one by one to make sure that Spark can read data from Hive.

```sql
scala> sql("select userid from mooc.users").show
scala> sql("select courseid, coursename from mooc.courses").show
scala> sql("select actionid, srcid, dstid, duration, feature0, feature1, feature2, feature3, label from mooc.actions").show
```

Here is an example of data read from the `mooc.actions` table.

```sql
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
|actionid|srcid|               dstid|duration|    feature0|    feature1|   feature2|   feature3|label|
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
|       0|    0|Environmental Dis...|     0.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       1|    0|  History of Ecology|     6.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       2|    0|      Women in Islam|    41.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       3|    0|  History of Ecology|    49.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       4|    0|      Women in Islam|    51.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       5|    0|Legacies of the A...|    55.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       6|    0|          ITP Core 2|    59.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       7|    0|The Research Pape...|    62.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       8|    0|        Neurobiology|    65.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|       9|    0|           Wikipedia|   113.0|-0.319991479|-0.435701433|1.108826104|12.77723482|false|
|      10|    0|Media History and...|   226.0|-0.319991479|-0.435701433|0.607804941|149.4512115|false|
|      11|    0|             WIKISOO|   974.0|-0.319991479|-0.435701433|1.108826104|3.344522776|false|
|      12|    0|Environmental Dis...|  1000.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      13|    0|             WIKISOO|  1172.0|-0.319991479|-0.435701433|1.108826104|1.136866766|false|
|      14|    0|      Women in Islam|  1182.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      15|    0|  History of Ecology|  1185.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      16|    0|Human Development...|  1687.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      17|    1|Human Development...|  7262.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      18|    1|  History of Ecology|  7266.0|-0.319991479|-0.435701433|0.106783779|-0.06730924|false|
|      19|    1|      Women in Islam|  7273.0|-0.319991479|-0.435701433|0.607804941|0.936170765|false|
+--------+-----+--------------------+--------+------------+------------+-----------+-----------+-----+
only showing top 20 rows
```

### Step 3. Edit configuration file

After compiling of Exchange, copy the `target/classes/application.conf` file and edit the configuration for Hive. In this example, a new configuration file is named `hive_ application.conf`. In this file, the vertex and edge related configuration is introduced as comments and all the items that are not used in this example are commented out. For more information about the Spark and Nebula related parameters, see [Spark related parameters](../parameter-reference/ex-ug-paras-spark.md) and [Nebula Graph related parameters](../parameter-reference/ex-ug-paras-nebulagraph.md).

```conf
{
  # Spark related configuration
  spark: {
    app: {
      name: Nebula Exchange 2.0
    }
    driver: {
      cores: 1
      maxResultSize: 1G
    }
    executor: {
        memory:1G
    }
    cores {
      max: 16
    }
  }

  # If Spark and Hive are deployed in the different clusters, 
  # configure these parameters for Hive. Otherwise, ignore them. 
  #hive: {
  #  waredir: "hdfs://NAMENODE_IP:9000/apps/svr/hive-xxx/warehouse/"
  #  connectionURL: "jdbc:mysql://your_ip:3306/hive_spark?characterEncoding=UTF-8"
  #  connectionDriverName: "com.mysql.jdbc.Driver"
  #  connectionUserName: "user"
  #  connectionPassword: "password"
  #}

  # Nebula Graph related configuration
  nebula: {
    address:{
      # Specifies the IP addresses and ports of the Graph Service and the Meta Service of Nebula Graph
      # If multiple servers are used, separate the addresses with commas.
      # Format: "ip1:port","ip2:port","ip3:port"
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }

    # Specifies an account that has the WriteData privilege in Nebula Graph and its password
    user: user
    pswd: password

    # Specifies a graph space name
    space: hive
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

  # Process vertices
  tags: [
    # Sets for the user tag
    {
      # Specifies a tag name defined in Nebula Graph
      name: user
      type: {
        # Specifies the data source. hive is used.
        source: hive
        # Specifies how to import vertex data into Nebula Graph: client or sst.
        # For more information about importing sst files, see Import SST files (doc to do).
        sink: client
      }

      # Specifies the SQL statement to read data from the users table in the mooc database
      exec: "select userid from mooc.users"

      # Specifies the column names from the users table to fields. 
      # Their values are used as the source of the userId (nebula.fields) property defined in Nebula Graph.
      # If more than one column name is specified, separate them with commas.
      # fields for the Hive and nebula.fields for Nebula Graph must have the one-to-one correspondence relationship.
      fields: [userid]
      nebula.fields: [userId]

      # Specifies a column as the source of VIDs.
      # The value of vertex must be one column name in the exec sentence. 
      # If the values are not of the int type, use vertex.policy to 
      # set the mapping policy. "hash" is preferred. 
      # Refer to the configuration of the course tag.
      vertex: userid

      # Specifies the maximum number of vertex data to be written into 
      # Nebula Graph in a single batch.
      batch: 256

      # Specifies the partition number of Spark.
      partition: 32
    }

    # Sets for the course tag
    {
      name: course
      type: {
        source: hive
        sink: client
      }
      exec: "select courseid, coursename from mooc.courses"
      fields: [courseid, coursename]
      nebula.fields: [courseId, courseName]

      # Specifies a column as the source of VIDs.
      # The value of vertex.field must be one column name in the exec sentence. 
      vertex: coursename
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use vertex.policy for mapping.
      #vertex: {
      #  field: coursename
      #  policy: "hash"
      #}
      batch: 256
      partition: 32
    }
    # If more tags are necessary, refer to the preceding configuration to add more.
  ]

  # Process edges
  edges: [
    # Sets for the action edge type
    {
      # Specifies an edge type name defined in Nebula Graph
      name: action

      type: {
        # Specifies the data source. hive is used.
        source: hive

        # Specifies how to import vertex data into Nebula Graph: client or sst
        # For more information about importing sst files, 
        # see Import SST files (doc to do).
        sink: client
      }

      # Specifies the SQL statement to read data from the actions table in
      # the mooc database.
      exec: "select actionid, srcid, dstid, duration, feature0, feature1, feature2, feature3, label from mooc.actions"

      # Specifies the column names from the actions table to fields. 
      # Their values are used as the source of the properties of 
      # the action edge type defined in Nebula Graph.
      # If more than one column name is specified, separate them with commas.
      # fields for the Hive and nebula.fields for Nebula Graph must 
      # have the one-to-one correspondence relationship.
      fields: [actionid, duration, feature0, feature1, feature2, feature3, label]
      nebula.fields: [actionId, duration, feature0, feature1, feature2, feature3, label]

      # source specifies a column as the source of the IDs of
      # the source vertex of an edge.
      # target specifies a column as the source of the IDs of
      # the target vertex of an edge.
      # The value of source.field and target.field must be
      # column names set in the exec sentence. 
      source: srcid
      target: dstid
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use vertex.policy for mapping.
      #target: {
      #  field: dstid
      #  policy: "hash"
      #}

      # Specifies the maximum number of vertex data to be 
      # written into Nebula Graph in a single batch.
      batch: 256

      # Specifies the partition number of Spark.
      partition: 32
    }
  ]
}
```

### Step 4. (Optional) Verify the configuration

After the configuration, run the import command with the `-D` parameter to verify the configuration file. For more information about the parameters, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange /path/to/nebula-exchange-2.0.0.jar -c /path/to/conf/hive_application.conf -h -D
```

### Step 5. Import data into Nebula Graph

When the configuration is ready, run this command to import data from HIVE into Nebula Graph. For more information about the parameters, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange /path/to/nebula-exchange-2.0.0.jar -c /path/to/conf/hive_application.conf -h
```

### Step 6. (Optional) Verify data in Nebula Graph

You can use a Nebula Graph client, such as Nebula Graph Studio, to verify the imported data. For example, in Nebula Graph Studio, run this statement.

```ngql
GO FROM "1" OVER action;
```

If the queried destination vertices return, the data are imported into Nebula Graph.

You can run the [`SHOW STATS`](../../3.ngql-guide/7.general-query-statements/6.show/14.show-stats.md) statement to count the data.

### Step 7. (Optional) Create and rebuild indexes in Nebula Graph

After the data is imported, you can create and rebuild indexes in Nebula Graph. For more information, see [nGQL User Guide](../../3.ngql-guide/14.native-index-statements/1.create-native-index.md).
