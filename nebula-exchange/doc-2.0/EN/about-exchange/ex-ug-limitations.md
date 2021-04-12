# Limitations

This article introduces the limitations of Exchange v2.x.

## Supported Nebula Graph versions

Exchange v2.x supports Nebula Graph v2.x only. If you are using Nebula Graph v1.x, please use [Nebula Exchange v1.x](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools "Click to go to GitHub").

## Supported operation systems

You can use Exchange v2.x in these operation systems:

- CentOS 7
- macOS

> **NOTE**: Importing SST files with Exchange v2.x is supported in Linux only.

## Software dependencies

To make sure that Exchange v2.x works properly, make sure that these software applications are installed in your machine:

- Apache Spark: 2.3.0 or later versions

- Java: 1.8

- Scala: 2.10.7, 2.11.12, or 2.12.10

In these scenarios, Hadoop Distributed File System (HDFS) must be deployed:

- Importing data from HDFS to Nebula Graph

- Importing SST files into Nebula Graph
