/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm
import com.vesoft.nebula.connector.connector.{NebulaDataFrameReader}
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ReadData {

  /**
    * read edge data from local csv and apply clustering coefficient
    * livejournal data: https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
    *
    * The livejournal data is put to hdfs, the path is
    * hdfs://127.0.0.1:9000/user/root/livejournal/soc-LiveJournal1.txt
    */
  def readLiveJournalData(spark: SparkSession): DataFrame = {
    val df = spark.sparkContext.textFile(
      "hdfs://127.0.0.1:9000/user/root/livejournal/soc-LiveJournal1.txt")

    val dd = df
      .map(line => {
        (line.trim.split("\t")(0), line.trim.split("\t")(1))
      })
      .map(row => Row(row._1, row._2))

    val schema = StructType(
      List(StructField("src", StringType, nullable = false),
           StructField("dst", StringType, nullable = true)))
    val edgeDF = spark.sqlContext.createDataFrame(dd, schema)
    edgeDF
  }

  /**
    * read edge data from csv
    */
  def readCsvData(spark: SparkSession): DataFrame = {
    val df = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("example/src/main/resources/data.csv")
    df
  }

  /**
    * read edge data from csv
    * the data has string type id
    */
  def readStringCsvData(spark: SparkSession): DataFrame = {
    val df = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("example/src/main/resources/string_data.csv")
    df
  }

  /**
    * read edge data from Nebula
    */
  def readNebulaData(spark: SparkSession): DataFrame = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("knows")
      .withNoColumn(true)
      .withLimit(2000)
      .withPartitionNum(100)
      .build()
    val df: DataFrame = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    df
  }
}
