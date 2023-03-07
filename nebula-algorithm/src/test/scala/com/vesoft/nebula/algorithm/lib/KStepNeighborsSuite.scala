/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package scala.com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.KNeighborsConfig
import com.vesoft.nebula.algorithm.lib.KStepNeighbors
import org.apache.spark.sql.SparkSession
import org.junit.Test

class KStepNeighborsSuite {
  @Test
  def kcoreSuite(): Unit = {
    val spark =
      SparkSession.builder().master("local").config("spark.sql.shuffle.partitions", 5).getOrCreate()
    val data        = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val kStepConfig = new KNeighborsConfig(List(1, 2, 3), 1)
    KStepNeighbors.apply(spark, data, kStepConfig)
  }
}
