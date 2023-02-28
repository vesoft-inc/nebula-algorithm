/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.LPAConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class LabelPropagationAlgoSuite {
  @Test
  def lpaAlgoSuite(): Unit = {
    val spark =
      SparkSession.builder().master("local").config("spark.sql.shuffle.partitions", 5).getOrCreate()
    val data      = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val lpaConfig = new LPAConfig(5)
    val result    = LabelPropagationAlgo.apply(spark, data, lpaConfig, false)
    assert(result.count() == 4)
    result.foreach(row => {
      assert(row.get(1).toString.toInt == 1)
    })

    val encodeLpaConfig = new LPAConfig(5)
    assert(LabelPropagationAlgo.apply(spark, data, encodeLpaConfig, false).count() == 4)
  }
}
