/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.algorithm.config.{CcConfig, PRConfig}
import com.vesoft.nebula.algorithm.lib.{PageRankAlgo, StronglyConnectedComponentsAlgo}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PageRankExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    // the edge data has numerical vid
    val csvDF = ReadData.readCsvData(spark)
    pagerank(spark, csvDF)

    // the edge data has string vid
    val stringCsvDF = ReadData.readStringCsvData(spark)
    pagerankWithIdMaping(spark, stringCsvDF)
  }

  /**
    * the src id and dst id are numerical values
    */
  def pagerank(spark: SparkSession, df: DataFrame): Unit = {
    val pageRankConfig = PRConfig(3, 0.85)
    val pr             = PageRankAlgo.apply(spark, df, pageRankConfig, false)
    pr.show()
  }

  /**
    * convert src id and dst id to numerical value
    */
  def pagerankWithIdMaping(spark: SparkSession, df: DataFrame): Unit = {
    val encodedDF      = convertStringId2LongId(df)
    val pageRankConfig = PRConfig(3, 0.85)
    val pr             = PageRankAlgo.apply(spark, encodedDF, pageRankConfig, false)
    val decodedPr      = reconvertLongId2StringId(spark, pr)
    decodedPr.show()
  }

  /**
    * if your edge data has String type src_id and dst_id, then you need to convert the String id to Long id.
    *
    * in this example, the columns of edge dataframe is: src, dst
    *
    */
  def convertStringId2LongId(dataframe: DataFrame): DataFrame = {
    // get all vertex ids from edge dataframe
    val srcIdDF: DataFrame = dataframe.select("src").withColumnRenamed("src", "id")
    val dstIdDF: DataFrame = dataframe.select("dst").withColumnRenamed("dst", "id")
    val idDF               = srcIdDF.union(dstIdDF).distinct()
    idDF.show()

    // encode id to Long type using dense_rank, the encodeId has two columns: id, encodedId
    // then you need to save the encodeId to convert back for the algorithm's result.
    val encodeId = idDF.withColumn("encodedId", dense_rank().over(Window.orderBy("id")))
    encodeId.write.option("header", true).csv("file:///tmp/encodeId.csv")
    encodeId.show()

    // convert the edge data's src and dst
    val srcJoinDF = dataframe
      .join(encodeId)
      .where(col("src") === col("id"))
      .drop("src")
      .drop("id")
      .withColumnRenamed("encodedId", "src")
    srcJoinDF.cache()
    val dstJoinDF = srcJoinDF
      .join(encodeId)
      .where(col("dst") === col("id"))
      .drop("dst")
      .drop("id")
      .withColumnRenamed("encodedId", "dst")
    dstJoinDF.show()

    // make the first two columns of edge dataframe are src and dst id
    dstJoinDF.select("src", "dst", "weight")
  }

  /**
    * re-convert the algorithm's result
    * @return dataframe with columns: id, {algo_name}
    */
  def reconvertLongId2StringId(spark: SparkSession, dataframe: DataFrame): DataFrame = {
    // the String id and Long id map data
    val encodeId = spark.read.option("header", true).csv("file:///tmp/encodeId.csv")

    encodeId
      .join(dataframe)
      .where(col("encodedId") === col("_id"))
      .drop("encodedId")
      .drop("_id")
  }
}
