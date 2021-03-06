/*
 * Copyright (c) 2021. vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.HanpConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class HanpSuite {
  @Test
  def hanpSuite() = {
    val spark      = SparkSession.builder().master("local").getOrCreate()
    val data       = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val hanpConfig = new HanpConfig(0.1, 10, 1.0)
    val result     = HanpAlgo.apply(spark, data, hanpConfig, false)
    assert(result.count() == 4)
  }
}
