/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{
  AlgoConstants,
  ShortestPathConfig
}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ShortestPathAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  /**
    * run the ShortestPath algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            shortestPathConfig: ShortestPathConfig,
            hasWeight: Boolean): DataFrame = {

    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)

    val prResultRDD = execute(graph, shortestPathConfig.landmarks)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.SHORTPATH_RESULT_COL, StringType, nullable = true)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(prResultRDD, schema)

    algoResult
  }

  def execute(graph: Graph[None.type, Double], landmarks: Seq[VertexId]): RDD[Row] = {
    val spResultRDD: VertexRDD[SPMap] = ShortestPaths.run(graph, landmarks).vertices
    spResultRDD.map(row => Row(row._1, row._2.toString()))
  }
}
