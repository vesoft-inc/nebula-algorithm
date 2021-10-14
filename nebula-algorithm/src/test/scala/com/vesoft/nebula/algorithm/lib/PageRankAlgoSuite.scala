/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{Configs, PRConfig, SparkConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class PageRankAlgoSuite {
  @Test
  def pageRankSuite(): Unit = {
    val spark         = SparkSession.builder().master("local").getOrCreate()
    val data          = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val prConfig      = new PRConfig(5, 1.0)
    val louvainResult = PageRankAlgo.apply(spark, data, prConfig, false)
    assert(louvainResult.count() == 4)
  }
}
