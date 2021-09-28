/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  WriteMode,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

object NebulaSparkWriterExample {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    writeVertex(spark)
    writeEdge(spark)

    updateVertex(spark)
    updateEdge(spark)

    deleteVertex(spark)
    deleteEdge(spark)

    spark.close()
  }

  /**
    * for this example, your nebula tag schema should have property names: name, age, born
    * if your withVidAsProp is true, then tag schema also should have property name: id
    */
  def writeVertex(spark: SparkSession): Unit = {
    LOG.info("start to write nebula vertices")
    val df = spark.read.json("example/src/main/resources/vertex")
    df.show()

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  /**
    * for this example, your nebula edge schema should have property names: descr, timp
    * if your withSrcAsProperty is true, then edge schema also should have property name: src
    * if your withDstAsProperty is true, then edge schema also should have property name: dst
    * if your withRankAsProperty is true, then edge schema also should have property name: degree
    */
  def writeEdge(spark: SparkSession): Unit = {
    LOG.info("start to write nebula edges")
    val df = spark.read.json("example/src/main/resources/edge")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  /**
    * We only update property that exists in DataFrame. For this example, update property `name`.
    */
  def updateVertex(spark: SparkSession): Unit = {
    LOG.info("start to write nebula vertices")
    val df = spark.read.json("example/src/main/resources/vertex").select("id", "age")
    df.show()

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(1000)
      .withWriteMode(WriteMode.UPDATE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  /**
    * we only update property that exists in DataFrame. For this example, we only update property `descr`.
    * if withRankField is not set when execute {@link writeEdge}, then don't set it too in this example.
    */
  def updateEdge(spark: SparkSession): Unit = {
    LOG.info("start to write nebula edges")
    val df = spark.read
      .json("example/src/main/resources/edge")
      .select("src", "dst", "degree", "descr")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .withWriteMode(WriteMode.UPDATE)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  def deleteVertex(spark: SparkSession): Unit = {
    LOG.info("start to delete nebula vertices")
    val df = spark.read
      .json("example/src/main/resources/vertex")
      .select("id")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withVidField("id")
      .withBatch(1)
      .withUser("root")
      .withPasswd("nebula")
      .withWriteMode(WriteMode.DELETE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  def deleteEdge(spark: SparkSession): Unit = {
    LOG.info("start to delete nebula edges")
    val df = spark.read
      .json("example/src/main/resources/edge")
      .select("src", "dst", "degree")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withBatch(10)
      .withWriteMode(WriteMode.DELETE)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

}
