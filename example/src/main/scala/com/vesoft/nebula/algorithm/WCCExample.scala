/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.{AlgoConstants, CcConfig}
import com.vesoft.nebula.algorithm.lib.ConnectedComponentsAlgo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object WCCExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    // val csvDF     = ReadData.readCsvData(spark)
    // val nebulaDF  = ReadData.readNebulaData(spark)
    val journalDF = ReadData.readLiveJournalData(spark)

    wcc(spark, journalDF)
  }

  def wcc(spark: SparkSession, df: DataFrame): Unit = {
    val ccConfig = CcConfig(Int.MaxValue)
    val cc       = ConnectedComponentsAlgo.apply(spark, df, ccConfig, false)
    cc.show()
  }

  def wccStatis(spark: SparkSession, df: DataFrame): Unit = {
    val ccConfig = CcConfig(Int.MaxValue)
    val cc       = ConnectedComponentsAlgo.apply(spark, df, ccConfig, false)
    cc.show()

    // compute the nodes number in largest WCC, for livejournal dataset, the value is 4843953
    import spark.implicits._
    val ccStatistic = cc
      .select(AlgoConstants.CC_RESULT_COL)
      .map(row => (row.get(0).toString, 1))
      .rdd
      .reduceByKey(_ + _)
      .sortBy(kv => (kv._2), false)
      .first()
    println(ccStatistic)
  }
}
