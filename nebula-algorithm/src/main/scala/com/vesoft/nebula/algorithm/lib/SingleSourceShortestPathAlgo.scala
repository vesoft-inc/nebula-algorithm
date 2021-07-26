/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, SingleSourceShortestPathConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SingleSourceShortestPathAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)
  val ALGORITHM: String = "SingleSourceShortestPath"

  /**
   * run the SingleSourceShortestPath algorithm for nebula graph
   */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            singleSourceShortestPathConfig: SingleSourceShortestPathConfig,
            hasWeight:Boolean):DataFrame={
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)
    val ssspGraph                = execute(graph, singleSourceShortestPathConfig.sourceId)
    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.SINGLESOURCESHORTESTPATH_RESULT_COL, DoubleType, nullable = true)
      ))
    val resultRDD=ssspGraph.vertices.map(vertex => Row(vertex._1, vertex._2))
    val algoResult=spark.sqlContext.createDataFrame(resultRDD,schema)
    algoResult
  }

  /**
   * execute SingleSourceShortestPath algorithm
   */
  def execute(graph: Graph[None.type, Double],
              sourceId: graphx.VertexId):Graph[Double,Double]={
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val ssspGraph = initialGraph.pregel(Double.PositiveInfinity)(
      (_, dist, newDist) => math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    ssspGraph
  }

}
