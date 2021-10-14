/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import org.apache.spark.sql.SparkSession
import org.junit.Test

class DegreeStaticAlgoSuite {
  @Test
  def degreeStaticAlgoSuite(): Unit = {
    val spark  = SparkSession.builder().master("local").getOrCreate()
    val data   = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val result = DegreeStaticAlgo.apply(spark, data)
    assert(result.count() == 4)
    result.foreach(row => {
      assert(row.get(1).toString.toInt == 8)
      assert(row.get(2).toString.toInt == 4)
      assert(row.get(3).toString.toInt == 4)
    })
  }
}
