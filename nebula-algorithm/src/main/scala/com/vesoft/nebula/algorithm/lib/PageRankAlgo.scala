/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, PRConfig}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PageRankAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "PageRank"

  /**
    * run the pagerank algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            pageRankConfig: PRConfig,
            hasWeight: Boolean): DataFrame = {

    var encodeIdDf: DataFrame = null

    val graph: Graph[None.type, Double] = if (pageRankConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, hasWeight)
      encodeIdDf = encodeId
      NebulaUtil.loadInitGraph(data, hasWeight)
    } else {
      NebulaUtil.loadInitGraph(dataset, hasWeight)
    }

    val prResultRDD = execute(graph, pageRankConfig.maxIter, pageRankConfig.resetProb)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.PAGERANK_RESULT_COL, DoubleType, nullable = true)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(prResultRDD, schema)

    if (pageRankConfig.encodeId) {
      DecodeUtil.convertAlgoId2StringId(algoResult, encodeIdDf)
    } else {
      algoResult
    }
  }

  def execute(graph: Graph[None.type, Double], maxIter: Int, resetProb: Double): RDD[Row] = {
    val prResultRDD: VertexRDD[Double] = PageRank.run(graph, maxIter, resetProb).vertices
    prResultRDD.map(row => Row(row._1, row._2))
  }
}
