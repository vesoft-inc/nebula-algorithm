/*
 * Copyright (c) 2021. vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.Node2vecConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class Node2vecAlgoSuite {
  @Test
  def node2vecAlgoSuite():Unit={
    val spark             = SparkSession.builder().master("local").getOrCreate()
    val data              = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val node2vecConfig = new Node2vecConfig(10,
      0.025,
      10,
      10,
      10,
      3,
      5,
      3,
      1.0,
      1.0,
      false,
      10,
      ",",
      "src/test/resources/model"
    )
    val result            = Node2vecAlgo.apply(spark, data, node2vecConfig, true)
    assert(result.count() == 4)
  }
}
