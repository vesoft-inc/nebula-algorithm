/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
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

    spark.close()
    sys.exit()
  }

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
      .withVidAsProp(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

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
        .build
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withSrcAsProperty(true)
      .withDstAsProperty(true)
      .withRankAsProperty(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

}
