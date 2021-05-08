/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.LPAConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class LabelPropagationAlgoSuite {
  @Test
  def lpaAlgoSuite(): Unit = {
    val spark     = SparkSession.builder().master("local").getOrCreate()
    val data      = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val lpaConfig = new LPAConfig(5)
    val result    = LabelPropagationAlgo.apply(spark, data, lpaConfig, false)
    assert(result.count() == 4)
    result.foreach(row => {
      assert(row.get(1).toString.toInt == 1)
    })
  }
}
