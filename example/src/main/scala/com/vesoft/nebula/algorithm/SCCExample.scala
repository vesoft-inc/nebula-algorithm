/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.{AlgoConstants, CcConfig}
import com.vesoft.nebula.algorithm.lib.{ConnectedComponentsAlgo, StronglyConnectedComponentsAlgo}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SCCExample {
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

    scc(spark, journalDF)
  }

  def scc(spark: SparkSession, df: DataFrame): Unit = {
    val ccConfig = CcConfig(Int.MaxValue)
    val scc      = StronglyConnectedComponentsAlgo.apply(spark, df, ccConfig, false)
    scc.show()
  }

  def sccStatis(spark: SparkSession, df: DataFrame): Unit = {
    val ccConfig = CcConfig(Int.MaxValue)
    val scc      = StronglyConnectedComponentsAlgo.apply(spark, df, ccConfig, false)
    scc.show()

    // compute the nodes number in largest SCC, for livejournal dataset, the value is 3828682
    import spark.implicits._
    val ccStatistic = scc
      .select(AlgoConstants.SCC_RESULT_COL)
      .map(row => (row.get(0).toString, 1))
      .rdd
      .reduceByKey(_ + _)
      .sortBy(kv => (kv._2), false)
      .first()
    println(ccStatistic)
  }
}
