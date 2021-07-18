package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.ClosenessConfig
import org.apache.spark.sql.SparkSession
import org.junit.Test

class ClosenessAlgoSuite {
  @Test
  def closenessAlgoSuite()={
    val spark             = SparkSession.builder().master("local").getOrCreate()
    val data              = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val closenessConfig = new ClosenessConfig(1)
    val result            = ClosenessAlgo.apply(spark, data, closenessConfig, true)
    assert(result.count() == 1)
  }
}
