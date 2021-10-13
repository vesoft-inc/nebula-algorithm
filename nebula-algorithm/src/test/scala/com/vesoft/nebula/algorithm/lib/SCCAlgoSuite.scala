/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.CcConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class SCCAlgoSuite {
  @Test
  def sccAlgoSuite(): Unit = {
    val spark     = SparkSession.builder().master("local").getOrCreate()
    val data      = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val sccConfig = new CcConfig(5)
    val sccResult = StronglyConnectedComponentsAlgo.apply(spark, data, sccConfig, true)
    assert(sccResult.count() == 4)
  }
}
