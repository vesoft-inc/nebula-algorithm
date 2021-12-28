/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{JaccardConfig, KCoreConfig}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class JaccardAlgoSuite {
  @Test
  def kcoreSuite(): Unit = {
    val spark         = SparkSession.builder().master("local").getOrCreate()
    val data          = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val jaccardConfig = new JaccardConfig(0.01)
    val jaccardResult = JaccardAlgo.apply(spark, data, jaccardConfig)
    jaccardResult.show()
    assert(jaccardResult.count() == 6)
  }
}
