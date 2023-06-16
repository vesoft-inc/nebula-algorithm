/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package scala.com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{BfsConfig, DfsConfig}
import com.vesoft.nebula.algorithm.lib.{BfsAlgo, DfsAlgo}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class DfsAlgoSuite {
  @Test
  def bfsAlgoSuite(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()
    val data          = spark.read.option("header", true).csv("src/test/resources/edge.csv")
    val dfsAlgoConfig = new DfsConfig(5, "3")
//    val result        = DfsAlgo.apply(spark, data, dfsAlgoConfig)
//    result.show()
//    assert(result.count() == 4)

    val encodeDfsConfig = new DfsConfig(5, "3", true)
    val encodeResult    = DfsAlgo.apply(spark, data, encodeDfsConfig)

    encodeResult.show()
    assert(encodeResult.count() == 4)
  }
}
