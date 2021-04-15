# Import data from CSV files

This article uses an example to show how to use Exchange to import data from CSV files stored on HDFS into Nebula Graph.

If you want to import data from local CSV files into Nebula Graph v2.x, see [Nebula Importer](https://github.com/vesoft-inc/nebula-importer "Click to go to GitHub").

## Dataset

In this article, the [Social Network: MOOC User Action Dataset](https://snap.stanford.edu/data/act-mooc.html "Click to go to Stanford Network Analysis Platform") provided by Stanford Network Analysis Platform (SNAP) and 97 unique course names obtained from the public network are used as the sample dataset. The dataset contains:

- Two vertex types (`user` and `course`), 7,144 vertices in total.
- One edge type (`action`), 411,749 edges in total.

You can download the example dataset from the [nebula-web-docker](https://github.com/vesoft-inc/nebula-web-docker/tree/master/example/mooc-actions "Click to go to GitHub") repository.

## Environment

The practice is done in macOS. Here is the environment information:

- Hardware specifications:
  - CPU: 1.7 GHz Quad-Core Intel Core i7
  - Memory: 16 GB

- Spark 2.4.7, deployed in the Standalone mode

- Hadoop 2.9.2, deployed in the Pseudo-Distributed mode

- Nebula Graph v2-nightly, deployed with Docker Compose. For more information, see [Deploy Nebula Graph with Docker Compose](https://github.com/vesoft-inc/nebula-docker-compose "Click to go to GitHub").

## Prerequisites

To import data from CSV files on HDFS with Exchange v2.x, do a check of these:

- Exchange v2.x is compiled. For more information, see [Compile Exchange v2.x](../ex-ug-compile.md). Exchange 2.0 is used in this example.

- Spark is installed.

- Hadoop is installed and started.

- Nebula Graph is deployed and started. Get the information:
  - IP addresses and ports of the Graph Service and the Meta Service.
  - A Nebula Graph account with the privilege of writing data and its password.

- Get the necessary information for schema creation in Nebula Graph, including tags and edge types.

## Procedure

### Step 1. Create a schema in Nebula Graph

Analyze the data in the CSV files and follow these steps to create a schema in Nebula Graph:

1. Confirm the essential elements of the schema.

    | Elements  | Names | Properties |
    | :--- | :--- | :--- |
    | Tag | `user` | `userId string` |
    | Tag | `course` | `courseId int, courseName string` |
    | Edge Type | `action` | `actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double` |

2. In Nebula Graph, create a graph space named **csv** and create a schema.

    ```ngql
    -- Create a graph space named csv
    CREATE SPACE [IF NOT EXISTS] <graph_space_name>
        [(partition_num = <partition_number>, 
        replica_factor = <replica_number>, 
        vid_type = {FIXED_STRING(<N>)) | INT64}];
    
    -- Choose the csv graph space
    USE csv;
    
    -- Create the user tag
    CREATE TAG user(userId string);
    
    -- Create the course tag
    CREATE TAG course(courseId int, courseName string);
    
    -- Create the action edge type
    CREATE EDGE action (actionId int, duration double, label bool, feature0 double, feature1 double, feature2 double, feature3 double);
    ```

For more information, see [Quick Start of Nebula Graph Database](https://docs.nebula-graph.io/2.0/2.quick-start/1.quick-start-workflow/).

### Step 2. Prepare CSV files

Do a check of these:

1. The CSV files are processed to meet the requirements of the schema.
    >**NOTE**: Exchange supports importing CSV files with or without headers.

2. The CSV files must be stored in HDFS and get the file storage path.

### Step 3. Edit configuration file

After compiling of Exchange, copy the `target/classes/application.conf` file and edit the configuration for CSV files. In this example, a new configuration file is named `csv_ application.conf`. In this file, the vertex and edge related configuration is introduced in the comments and all the items that are not used in this example are commented out. For more information about the Spark and Nebula related parameters, see [Spark related parameters](../parameter-reference/ex-ug-paras-spark.md) and [Nebula Graph related parameters](../parameter-reference/ex-ug-paras-nebulagraph.md).

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

  # Nebula Graph related configuration
  nebula: {
    address:{
      # Specifies the IP addresses and ports of the Graph Service and the Meta Service of Nebula Graph.
      # If multiple servers are used, separate the addresses with commas. 
      # Format: "ip1:port","ip2:port","ip3:port". 
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }

    # Specifies an account that has the WriteData privilege in Nebula Graph and its password.
    user: user
    pswd: password

    # Specifies a graph space name
    space: csv
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
    # Sets for the course tag
    {
      # Specifies a tag name defined in Nebula Graph.
      name: course
      type: {
        # Specifies the data source. csv is used.
        source: csv

        # Specifies how to import vertex data into Nebula Graph: client or sst.
        # For more information about importing sst files, see Import SST files (doc_to_do).
        sink: client
      }

      # Specifies the HDFS path of the CSV file. 
      # Enclose the path with double quotes and start the path with hdfs://.
      path: "hdfs://namenode_ip:port/path/to/course.csv"
      
      # If the CSV file has no header, use [_c0, _c1, _c2, ..., _cn] to 
      # represent its header and to indicate columns as the source of the property values.
      fields: [_c0, _c1]
      # If the CSV file has a header, use the actual column names.

      # Specifies property names defined in Nebula Graph.
      # fields for the CSV file and nebula.fields for Nebula Graph must 
      # have the one-to-one correspondence relationship.
      nebula.fields: [courseId, courseName]

      # Specifies a column as the source of VIDs.
      # The value of vertex must be one column of the CSV file.
      vertex: _c1
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use vertex.policy for mapping.
      # vertex: {
      #  field: _c1,
      #  policy: "hash"
      # }

      # Specifies the separator. The default value is commas.
      separator: ","

      # If the CSV file has a header, set header to true.
      # If the CSV file has no header, set header to false (default value).
      header: false

      # Specifies the maximum number of vertex data to be written into 
      # Nebula Graph in a single batch.
      batch: 256

      # Specifies the partition number of Spark.
      partition: 32
    }

    # Sets for the user tag
    {
      name: user
      type: {
        source: csv
        sink: client
      }
      path: "hdfs://namenode_ip:port/path/to/user.csv"

      # fields for the CSV file and nebula.fields for Nebula Graph must 
      # have the one-to-one correspondence relationship.
      fields: [userId]

      # Specifies property names defined in Nebula Graph.
      # fields for the CSV file and nebula.fields for Nebula Graph must 
      # have the one-to-one correspondence relationship.
      nebula.fields: [userId]

      # The value of vertex.field must be one column of the CSV file.
      vertex: userId
      separator: ","
      header: true
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
        # Specifies the data source. csv is used.
        source: csv

        # Specifies how to import vertex data into Nebula Graph: client or sst.
        # For more information about importing sst files, see Import SST files (doc_to_do).
        sink: client
      }

      # Specifies the HDFS path of the CSV file. 
      # Enclose the path with double quotes and start the path with hdfs://.
      path: "hdfs://namenode_ip:port/path/to/actions.csv"

      # If the CSV file has no header, use [_c0, _c1, _c2, ..., _cn] to 
      # represent its header and to indicate columns as the source of the property values.
      fields: [_c0, _c3, _c4, _c5, _c6, _c7, _c8]
      # If the CSV file has a header, use the actual column names.

      # Specifies property names defined in Nebula Graph.
      # fields for the CSV file and nebula.fields for Nebula Graph must 
      # have the one-to-one correspondence relationship.
      nebula.fields: [actionId, duration, feature0, feature1, feature2, feature3, label]

      # Specifies the columns as the source of the IDs of the source and target vertices.
      source: _c1
      target: _c2
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use source.policy or target.policy for mapping.
      #target: {
      #  field: _c2
      #  policy: "hash"
      #}

      # Specifies the separator. The default value is commas.      
      separator: ","

      # If the CSV file has a header, set header to true.
      # If the CSV file has no header, set header to false (default value).
      header: false

      # Specifies the maximum number of vertex data to be written into
      # Nebula Graph in a single batch.
      batch: 256

      # Specifies the partition number of Spark.
      partition: 32
    }
  ]
  # If more edge types are necessary, refer to the preceding configuration to add more.
}
```

### Step 4. Import data into Nebula Graph

When the configuration is ready, run this command to import data from CSV files into Nebula Graph. For more information about the parameters, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange /path/to/nebula-exchange-2.0.0.jar -c /path/to/conf/csv_application.conf 
```

### Step 5. (Optional) Verify data in Nebula Graph

You can use a Nebula Graph client, such as Nebula Graph Studio, to verify the imported data. For example, in Nebula Graph Studio, run this statement.

```ngql
GO FROM "1" OVER action;
```

If the queried destination vertices return, the data are imported into Nebula Graph.

You can run the [`SHOW STATS`](../../3.ngql-guide/7.general-query-statements/6.show/14.show-stats.md) statement to count the data.

### Step 6. (Optional) Create and rebuild indexes in Nebula Graph

After the data is imported, you can create and rebuild indexes in Nebula Graph. For more information, see [nGQL User Guide](../../3.ngql-guide/14.native-index-statements/1.create-native-index.md).
