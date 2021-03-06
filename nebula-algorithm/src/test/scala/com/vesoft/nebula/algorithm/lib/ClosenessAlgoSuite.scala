/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import org.apache.spark.sql.SparkSession
import org.junit.Test

class ClosenessAlgoSuite {
  @Test
  def closenessAlgoSuite() = {
    val spark  = SparkSession.builder().master("local").getOrCreate()
    val data   = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val result = ClosenessAlgo.apply(spark, data, true)
    assert(result.count() == 4)
  }
}
