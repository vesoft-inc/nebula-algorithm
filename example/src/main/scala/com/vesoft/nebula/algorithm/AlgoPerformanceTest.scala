/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.vesoft.nebula.connector.connector.{NebulaDataFrameReader}
import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.{CcConfig, LPAConfig, LouvainConfig, PRConfig}
import com.vesoft.nebula.algorithm.lib.{
  ConnectedComponentsAlgo,
  LabelPropagationAlgo,
  LouvainAlgo,
  PageRankAlgo
}
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AlgoPerformanceTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = readNebulaData(spark)
    lpa(spark, df)
    louvain(spark, df)
    pagerank(spark, df)
    wcc(spark, df)

  }

  def readNebulaData(spark: SparkSession): DataFrame = {
    val start = System.currentTimeMillis()
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.0.1:9559")
        .withTimeout(6000)
        .withConnectionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("twitter")
      .withLabel("FOLLOW")
      .withNoColumn(true)
      .withLimit(20000)
      .withPartitionNum(120)
      .build()
    val df: DataFrame =
      spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    df.cache()
    df.count()
    println(s"read data cost time ${(System.currentTimeMillis() - start)}")
    df
  }

  def lpa(spark: SparkSession, df: DataFrame): Unit = {
    val start     = System.currentTimeMillis()
    val lpaConfig = LPAConfig(10)
    val lpa       = LabelPropagationAlgo.apply(spark, df, lpaConfig, false)
    lpa.write.csv("hdfs://127.0.0.1:9000/tmp/lpa")
    println(s"lpa compute and save cost ${System.currentTimeMillis() - start}")
  }

  def pagerank(spark: SparkSession, df: DataFrame): Unit = {
    val start          = System.currentTimeMillis()
    val pageRankConfig = PRConfig(10, 0.85)
    val pr             = PageRankAlgo.apply(spark, df, pageRankConfig, false)
    pr.write.csv("hdfs://127.0.0.1:9000/tmp/pagerank")
    println(s"pagerank compute and save cost ${System.currentTimeMillis() - start}")
  }

  def wcc(spark: SparkSession, df: DataFrame): Unit = {
    val start    = System.currentTimeMillis()
    val ccConfig = CcConfig(20)
    val cc       = ConnectedComponentsAlgo.apply(spark, df, ccConfig, false)
    cc.write.csv("hdfs://127.0.0.1:9000/tmp/wcc")
    println(s"wcc compute and save cost ${System.currentTimeMillis() - start}")
  }

  def louvain(spark: SparkSession, df: DataFrame): Unit = {
    val start         = System.currentTimeMillis()
    val louvainConfig = LouvainConfig(10, 5, 0.5)
    val louvain       = LouvainAlgo.apply(spark, df, louvainConfig, false)
    louvain.write.csv("hdfs://127.0.0.1:9000/tmp/louvain")
    println(s"louvain compute and save cost ${System.currentTimeMillis() - start}")
  }

}
