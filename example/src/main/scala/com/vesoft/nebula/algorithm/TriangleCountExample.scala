/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.lib.TriangleCountAlgo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TriangleCountExample {

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

    graphTriangleCount(spark, journalDF)
  }

  def graphTriangleCount(spark: SparkSession, df: DataFrame): Unit = {
    val triangleCount = TriangleCountAlgo.apply(spark, df)
    triangleCount.show()
  }
}
