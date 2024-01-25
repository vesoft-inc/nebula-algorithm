/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, LPAConfig}
import com.vesoft.nebula.algorithm.utils.{DecodeUtil}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object LabelPropagationAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "LabelPropagation"

  /**
    * run the LabelPropagation algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            lpaConfig: LPAConfig,
            hasWeight: Boolean): DataFrame = {
    spark.sparkContext.setJobGroup(ALGORITHM, s"Running $ALGORITHM")

    var encodeIdDf: DataFrame = null

    val graph: Graph[None.type, Double] = if (lpaConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, hasWeight)
      encodeIdDf = encodeId
      NebulaUtil.loadInitGraph(data, hasWeight)
    } else {
      NebulaUtil.loadInitGraph(dataset, hasWeight)
    }

    val lpaResultRDD = execute(graph, lpaConfig.maxIter)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.LPA_RESULT_COL, LongType, nullable = true)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(lpaResultRDD, schema)

    if (lpaConfig.encodeId) {
      DecodeUtil.convertAlgoResultId2StringId(algoResult, encodeIdDf, AlgoConstants.LPA_RESULT_COL)
    } else {
      algoResult
    }
  }

  def execute(graph: Graph[None.type, Double], maxIter: Int): RDD[Row] = {
    val lpaResultRDD: VertexRDD[VertexId] = LabelPropagation.run(graph, maxIter).vertices
    lpaResultRDD.map(row => Row(row._1, row._2))
  }
}
