/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.BetweennessConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class BetweennessAlgoSuite {
  @Test
  def betweennessAlgoSuite(): Unit = {
    val spark             = SparkSession.builder().master("local").getOrCreate()
    val data              = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val betweennessConfig = new BetweennessConfig(5)
    val result            = BetweennessCentralityAlgo.apply(spark, data, betweennessConfig, false)
    assert(result.count() == 4)
  }
}
