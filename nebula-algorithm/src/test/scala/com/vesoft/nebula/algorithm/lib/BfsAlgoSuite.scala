/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{BfsConfig, CcConfig}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class BfsAlgoSuite {
  @Test
  def bfsAlgoSuite(): Unit = {
    val spark         = SparkSession.builder().master("local").getOrCreate()
    val data          = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val bfsAlgoConfig = new BfsConfig(5, 1)
    val result        = BfsAlgo.apply(spark, data, bfsAlgoConfig)
    result.show()
    assert(result.count() == 4)
  }
}
