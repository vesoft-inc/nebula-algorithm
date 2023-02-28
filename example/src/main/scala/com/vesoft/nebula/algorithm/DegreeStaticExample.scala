/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.DegreeStaticConfig
import com.vesoft.nebula.algorithm.lib.DegreeStaticAlgo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DegreeStaticExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    // val nebulaDF  = ReadData.readNebulaData(spark)
    val journalDF = ReadData.readLiveJournalData(spark)
    degree(spark, journalDF)

    val csvDF = ReadData.readStringCsvData(spark)
    degreeForStringId(spark, csvDF)
  }

  def degree(spark: SparkSession, df: DataFrame): Unit = {
    val degree = DegreeStaticAlgo.apply(spark, df)
    degree.show()
  }

  def degreeForStringId(spark: SparkSession, df: DataFrame): Unit = {
    val degreeConfig = new DegreeStaticConfig(true)
    val degree       = DegreeStaticAlgo.apply(spark, df, degreeConfig)
    degree.show()
  }
}
