/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{ConfigSuite, Configs, LouvainConfig, SparkConfig}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.Test

class LouvainAlgoSuite {
  @Test
  def louvainSuite(): Unit = {
    val spark         = SparkSession.builder().master("local").getOrCreate()
    val data          = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val louvainConfig = new LouvainConfig(5, 2, 1.0)
    val louvainResult = LouvainAlgo.apply(spark, data, louvainConfig, false)
    assert(louvainResult.count() == 4)
  }
}
