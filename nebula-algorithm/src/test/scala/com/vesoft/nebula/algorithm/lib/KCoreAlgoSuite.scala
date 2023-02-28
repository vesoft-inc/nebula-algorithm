/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.KCoreConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class KCoreAlgoSuite {
  @Test
  def kcoreSuite(): Unit = {
    val spark =
      SparkSession.builder().master("local").config("spark.sql.shuffle.partitions", 5).getOrCreate()
    val data        = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val kcoreConfig = new KCoreConfig(10, 3)
    val kcoreResult = KCoreAlgo.apply(spark, data, kcoreConfig)
    assert(kcoreResult.count() == 4)

    val encodeKcoreConfig = new KCoreConfig(10, 3, true)
    assert(KCoreAlgo.apply(spark, data, encodeKcoreConfig).count() == 4)
  }
}
