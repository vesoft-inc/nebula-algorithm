/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, CcConfig}
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.lib.{ConnectedComponents, StronglyConnectedComponents}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object StronglyConnectedComponentsAlgo {

  /**
    * run the StronglyConnectedComponents algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            ccConfig: CcConfig,
            hasWeight: Boolean): DataFrame = {

    var encodeIdDf: DataFrame = null

    val graph: Graph[None.type, Double] = if (ccConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, hasWeight)
      encodeIdDf = encodeId
      NebulaUtil.loadInitGraph(data, hasWeight)
    } else {
      NebulaUtil.loadInitGraph(dataset, hasWeight)
    }

    val ccResultRDD = execute(graph, ccConfig.maxIter)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.SCC_RESULT_COL, LongType, nullable = true)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(ccResultRDD, schema)

    if (ccConfig.encodeId) {
      DecodeUtil.convertAlgoResultId2StringId(algoResult, encodeIdDf, AlgoConstants.SCC_RESULT_COL)
    } else {
      algoResult
    }
  }

  def execute(graph: Graph[None.type, Double], maxIter: Int): RDD[Row] = {
    val ccResultRDD: VertexRDD[VertexId] = StronglyConnectedComponents.run(graph, maxIter).vertices
    ccResultRDD.map(row => Row(row._1, row._2))
  }

}
