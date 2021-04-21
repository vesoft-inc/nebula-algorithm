# Import data from JSON files

This article uses an example to show how to use Exchange to import data from JSON files stored on HDFS into Nebula Graph.

## Dataset

The JSON file (test.json) used in this example is like `{"source":string, "target":string, "likeness":double}`, representing a `like` relationship between `source` and `target`. 21,645 records in total.

Here are some sample data:

```json
{"source":53802643,"target":87847387,"likeness":0.34}
{"source":29509860,"target":57501950,"likeness":0.40}
{"source":97319348,"target":50240344,"likeness":0.77}
{"source":94295709,"target":8189720,"likeness":0.82}
{"source":78707720,"target":53874070,"likeness":0.98}
{"source":23399562,"target":20136097,"likeness":0.47}
```

## Environment

The practice is done in macOS. Here is the environment information:

- Hardware specifications:
  - CPU: 1.7 GHz Quad-Core Intel Core i7
  - Memory: 16 GB

- Spark 2.4.7, deployed in the Standalone mode

- Hadoop 2.9.2, deployed in the Pseudo-Distributed mode

- Nebula Graph v2-nightly, deployed with Docker Compose. For more information, see [Deploy Nebula Graph with Docker Compose](https://github.com/vesoft-inc/nebula-docker-compose "Click to go to GitHub").

## Prerequisites

To import data from JSON files on HDFS with Exchange v2.x, do a check of these:

- Exchange v2.x is compiled. For more information, see [Compile Exchange v2.x](../ex-ug-compile.md). Exchange 2.0.0 is used in this example.

- Spark is installed.

- Hadoop is installed and started.

- Nebula Graph is deployed and started. Get the information:
  - IP addresses and ports of the Graph Service and the Meta Service.
  - A Nebula Graph account with the privilege of writing data and its password.

- Get the necessary information for schema creation in Nebula Graph, including tags and edge types.

## Procedure

### Step 1. Create a schema in Nebula Graph

Analyze the data in the JSON files and follow these steps to create a schema in Nebula Graph:

1. Confirm the essential elements of the schema.

    | Elements  | Names | Properties |
    | :--- | :--- | :--- |
    | Tag | `source` | `srcId string` |
    | Tag | `target` | `dstId string` |
    | Edge Type | `like` | `likeness double` |

2. In Nebula Graph, create a graph space named **json** and create a schema.

    ```ngql
    -- Create a graph space named json
    CREATE SPACE json(partition_num=10, replica_factor=1, vid_type=fixed_string(30));

    -- Choose the json graph space
    USE json;

    -- Create the source tag
    CREATE TAG source (srcId string);

    -- Create the target tag
    CREATE TAG target (dstId string);

    -- Create the like edge type
    CREATE EDGE like (likeness double);
    ```

For more information, see [Quick Start of Nebula Graph](../../2.quick-start/4.nebula-graph-crud.md).

### Step 2. Prepare JSON files

Create separate JSON files for vertex and edge data. Store the JSON files in HDFS and get the HDFS path of the files.

> **NOTE**: In this example, only one JSON file is used to import vertex and edge data at the same time. Some vertex data representing source and target are duplicate. Therefore, during the import process, these vertices are written repeatedly. In Nebula Graph, data is overwritten when repeated insertion occurs, and the last write is read out. In practice, to increase the write speed, creating separate files for vertex and edge data is recommended.

### Step 3. Edit configuration file

After compiling of Exchange, copy the `target/classes/application.conf` file and edit the configuration for JSON files. In this example, a new configuration file is named `json_ application.conf`. In this file, the vertex and edge related configuration is introduced as comments and all the items that are not used in this example are commented out. For more information about the Spark and Nebula related parameters, see [Spark related parameters](../parameter-reference/ex-ug-paras-spark.md) and [Nebula Graph related parameters](../parameter-reference/ex-ug-paras-nebulagraph.md).

```conf
{
  # Spark related configuration
  spark: {
    app: {
      name: Spark Writer
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
      # Format: "ip1:port","ip2:port","ip3:port"
      graph:["127.0.0.1:9669"]
      meta:["127.0.0.1:9559"]
    }
    # Specifies an account that has the WriteData privilege in Nebula Graph and its password.
    user: user
    pswd: password

    # Specifies a graph space name
    space: json

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
    # Sets for the source tag
    {
      # Specifies a tag name defined in Nebula Graph
      name: source
      type: {
        # Specifies the data source. json is used.
        source: json

        # Specifies how to import vertex data into Nebula Graph: client or sst.
        # For more information about importing sst files, see Import SST files (doc to do).
        sink: client
      }

      # Specifies the HDFS path of the JSON file. 
      # Enclose the path with double quotes and start the path with hdfs://.
      path: "hdfs://namenode_ip:port/path/to/test.json"

      # Specifies the keys in the JSON file. 
      # Their values are used as the source of the srcId property 
      # defined in Nebula Graph.
      # If more than one key is specified, separate them with commas.
      fields: ["source"]
      nebula.fields: ["srcId"]

      # Specifies the values of a key in the JSON file as 
      # the source of the VID in Nebula Graph.
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use vertex.policy for mapping.
      # vertex: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      vertex: source

      batch: 256
      partition: 32
    }
    # Sets for the target tag
    {
      name: target
      type: {
        source: json
        sink: client
      }
      path: "hdfs://namenode_ip:port/path/to/test.json"
      fields: ["target"]
      nebula.fields: ["dstId"]
      vertex: "target"
      batch: 256
      partition: 32
      isImplicit: true
    }
    # If more tags are necessary, refer to the preceding configuration to add more.
  ]

  # Process edges
  edges: [
    # Sets for the like edge type
    {
      # Specifies an edge type name defined in Nebula Graph
      name: like
      type: {
        # Specifies the data source. json is used.
        source: json

        # Specifies how to import vertex data into Nebula Graph: client or sst.
        # For more information about importing sst files, see Import SST files (doc to do).
        sink: client
      }

      # Specifies the HDFS path of the JSON file. 
      # Enclose the path with double quotes and start the path with hdfs://.
      path: "hdfs://namenode_ip:port/path/to/test.json"

      # Specifies the keys in the JSON file. 
      # Their values are used as the source of the likeness property defined in Nebula Graph.
      # If more than one key is specified, separate them with commas.
      fields: ["likeness"]
      nebula.fields: ["likeness"]

      # Specifies the values of two keys in the JSON file as the source
      # of the IDs of source and destination vertices of the like edges in Nebula Graph.
      # For now, only string type VIDs are supported in Nebula Graph v2.x. 
      # Do not use vertex.policy for mapping.
      # source: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      # target: {
      #   field: key_name_in_json
      #   policy: "hash"
      # }
      source: "source"
      target: "target"

      batch: 256
      partition: 32
    }
  # If more edge types are necessary, refer to the preceding configuration to add more.
  ]
}
```

### Step 4. (Optional) Verify the configuration

After the configuration, run the import command with the `-D` parameter to verify the configuration file. For more information about the parameters, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange /path/to/nebula-exchange-2.0.0.jar -c /path/to/conf/json_application.conf -D
```

### Step 5. Import data into Nebula Graph

When the configuration is ready, run this command to import data from JSON files into Nebula Graph. For more information about the parameters, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

```bash
$SPARK_HOME/bin/spark-submit --master "local" --class com.vesoft.nebula.exchange.Exchange /path/to/nebula-exchange-2.0.0.jar -c /path/to/conf/json_application.conf 
```

### Step 6. (Optional) Verify data in Nebula Graph

You can use a Nebula Graph client, such as Nebula Graph Studio, to verify the imported data. For example, in Nebula Graph Studio, run this statement.

```ngql
GO FROM "53802643" OVER like;
```

If the queried destination vertices return, the data are imported into Nebula Graph.

You can run the [`SHOW STATS`](../../3.ngql-guide/7.general-query-statements/6.show/14.show-stats.md) statement to count the data.

### Step 7. (Optional) Create and rebuild indexes in Nebula Graph

After the data is imported, you can create and rebuild indexes in Nebula Graph. For more information, see [nGQL User Guide](../../3.ngql-guide/1.nGQL-overview/1.overview.md).
