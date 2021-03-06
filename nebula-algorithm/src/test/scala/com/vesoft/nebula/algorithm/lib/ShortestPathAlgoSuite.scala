/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.ShortestPathConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class ShortestPathAlgoSuite {
  @Test
  def shortestPathAlgoSuite(): Unit = {
    val spark              = SparkSession.builder().master("local").getOrCreate()
    val data               = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val shortestPathConfig = new ShortestPathConfig(Seq(1, 2))
    val result             = ShortestPathAlgo.apply(spark, data, shortestPathConfig, false)
    assert(result.count() == 4)
  }
}
