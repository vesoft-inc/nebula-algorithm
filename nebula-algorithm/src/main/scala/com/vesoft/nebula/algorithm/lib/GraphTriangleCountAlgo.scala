/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.AlgoConstants
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
  *  compute all graph's triangle count
  */
object GraphTriangleCountAlgo {
  val ALGORITHM = "graphTriangleCount"

  def apply(spark: SparkSession, dataset: Dataset[Row]): DataFrame = {
    spark.sparkContext.setJobGroup(ALGORITHM, s"Running $ALGORITHM")

    val triangleCount = TriangleCountAlgo(spark, dataset)
    val count = triangleCount
      .select(AlgoConstants.TRIANGLECOUNT_RESULT_COL)
      .rdd
      .map(value => value.get(0).asInstanceOf[Int])
      .reduce(_ + _) / 3
    val list = List(count)
    val rdd  = spark.sparkContext.parallelize(list).map(row => Row(row))

    val schema = StructType(
      List(
        StructField("count", IntegerType, nullable = false)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(rdd, schema)

    algoResult
  }
}
