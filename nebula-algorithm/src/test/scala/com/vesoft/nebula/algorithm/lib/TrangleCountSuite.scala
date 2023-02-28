/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.TriangleConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class TrangleCountSuite {
  @Test
  def trangleCountSuite(): Unit = {
    val spark =
      SparkSession.builder().master("local").config("spark.sql.shuffle.partitions", 5).getOrCreate()
    val data               = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val trangleCountResult = TriangleCountAlgo.apply(spark, data)
    assert(trangleCountResult.count() == 4)
    assert(trangleCountResult.first().get(1) == 3)
    trangleCountResult.foreach(row => {
      assert(row.get(1) == 3)
    })

    val triangleConfig = TriangleConfig(true)
    assert(TriangleCountAlgo.apply(spark, data, triangleConfig).count() == 4)
  }
}
