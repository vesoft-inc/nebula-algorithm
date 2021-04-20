# Use Exchange

This article introduces the generally-used procedure on how to use Exchange to import data from a specified source to Nebula Graph.

## Prerequisites

- Nebula Graph is deployed and started. Get the information:
  - IP addresses and ports of the Graph Service and the Meta Service.
  - A Nebula Graph account with the privilege of writing data and its password.

- Exchange is compiled. For more information, see [Compile Exchange](../ex-ug-compile.md).

- Spark is installed.

- Get the necessary information for schema creation in Nebula Graph, including tags and edge types.

## Procedure

To import data from a source to Nebula Graph, follow these steps:

1. Create a graph space and a schema in Nebula Graph.

2. (Optional) Process the source data. For example, to import data from a Neo4j database, create indexes for the specified tags in Neo4j to export the data from Neo4j more quickly.

3. Edit the configuration file for Spark, Nebula Graph, vertices, and edges.
   > **NOTE**: After compiling of Exchange, refer to the example configuration files in the `nebula-exchange/target/classes` directory for the configuration for different sources.

4. (Optional) Run the import command with the `-D` parameter to verify the configuration. For more information, see [Import command parameters](../parameter-reference/ex-ug-para-import-command.md).

5. Run the import command to import data into Nebula Graph.

6. Verify the imported data in Nebula Graph.

7. (Optional) Create and rebuild indexes in Nebula Graph.

For more information, see the examples:

- [Import data from Hive](ex-ug-import-hive.md)
- [Import data from CSV files](ex-ug-import-from-csv.md)
- [Import data from JSON files](ex-ug-import-json.md)

<!---
client:parquet、ORC、neo4j、hbase、pulsar、kafka
SST
>