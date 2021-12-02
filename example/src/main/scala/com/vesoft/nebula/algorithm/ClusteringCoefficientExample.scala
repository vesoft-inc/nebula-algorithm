/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.CoefficientConfig
import com.vesoft.nebula.algorithm.lib.ClusteringCoefficientAlgo
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClusteringCoefficientExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val csvDF         = ReadData.readCsvData(spark)
    val nebulaDF      = ReadData.readNebulaData(spark)
    val liveJournalDF = ReadData.readLiveJournalData(spark)

    localClusteringCoefficient(spark, csvDF)
    globalCLusteringCoefficient(spark, csvDF)
  }

  /**
    * compute for local clustering coefficient
    */
  def localClusteringCoefficient(spark: SparkSession, df: DataFrame): Unit = {
    val localClusteringCoefficientConfig = new CoefficientConfig("local")
    val localClusterCoeff =
      ClusteringCoefficientAlgo.apply(spark, df, localClusteringCoefficientConfig)
    localClusterCoeff.show()
    localClusterCoeff
      .filter(row => !row.get(1).toString.equals("0.0"))
      .orderBy(col("clustercoefficient"))
      .write
      .option("header", true)
      .csv("hdfs://127.0.0.1:9000/tmp/ccresult")
  }

  /**
    * compute for global clustering coefficient
    */
  def globalCLusteringCoefficient(spark: SparkSession, df: DataFrame): Unit = {
    val globalClusteringCoefficientConfig = new CoefficientConfig("global")
    val globalClusterCoeff =
      ClusteringCoefficientAlgo.apply(spark, df, globalClusteringCoefficientConfig)
    globalClusterCoeff.show()
  }

}
