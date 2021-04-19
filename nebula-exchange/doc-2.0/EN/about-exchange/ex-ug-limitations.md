# Limitations

This article introduces the limitations of Exchange 2.0.

## Supported Nebula Graph versions

Exchange 2.0 supports Nebula Graph 2.0.0 only. If you are using Nebula Graph 1.x, please use [Nebula Exchange 1.x](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools "Click to go to GitHub").

## Supported operation systems

You can use Exchange 2.0 in these operation systems:

- CentOS 7
- macOS

> **NOTE**: Importing SST files with Exchange 2.0 is supported in Linux only.

## Software dependencies

To make sure that Exchange v2.x works properly, make sure that these software applications are installed in your machine:

- Apache Spark: 2.3.0 or later versions

- Java: 1.8

- Scala: 2.10.7, 2.11.12, or 2.12.10

In these scenarios, Hadoop Distributed File System (HDFS) must be deployed:

- Importing data from HDFS to Nebula Graph

- Generate SST files
