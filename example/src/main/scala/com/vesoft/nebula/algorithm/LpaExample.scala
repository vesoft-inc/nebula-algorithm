/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.LPAConfig
import com.vesoft.nebula.algorithm.lib.LabelPropagationAlgo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object LpaExample {
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

    lpa(spark, journalDF)
  }

  def lpa(spark: SparkSession, df: DataFrame): Unit = {
    val lpaConfig = LPAConfig(Int.MaxValue)
    val lpa       = LabelPropagationAlgo.apply(spark, df, lpaConfig, false)
    lpa.show()
  }
}
