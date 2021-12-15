/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, BfsConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Breadth-First Search for un-weight graph
  */
object BfsAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "BFS"

  /**
    * run the louvain algorithm for nebula graph
    */
  def apply(spark: SparkSession, dataset: Dataset[Row], bfsConfig: BfsConfig): DataFrame = {
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, false)
    val bfsGraph                        = execute(graph, bfsConfig.maxIter, bfsConfig.root)

    // filter out the not traversal vertices
    val visitedVertices = bfsGraph.vertices.filter(v => v._2 != Double.PositiveInfinity)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.BFS_RESULT_COL, DoubleType, nullable = true)
      ))
    val resultRDD = visitedVertices.map(vertex => Row(vertex._1, vertex._2))
    val algoResult = spark.sqlContext
      .createDataFrame(resultRDD, schema)
      .orderBy(col(AlgoConstants.BFS_RESULT_COL))
    algoResult
  }

  def execute(graph: Graph[None.type, Double], maxIter: Int, root: Long): Graph[Double, Double] = {
    val initialGraph = graph.mapVertices(
      (id, _) =>
        if (id == root) 0.0
        else Double.PositiveInfinity)

    // vertex program
    val vprog = { (id: VertexId, attr: Double, msg: Double) =>
      math.min(attr, msg)
    }

    val sendMsg = { (triplet: EdgeTriplet[Double, Double]) =>
      var iter: Iterator[(VertexId, Double)] = Iterator.empty
      val isSrcMarked                        = triplet.srcAttr != Double.PositiveInfinity
      val isDstMarked                        = triplet.dstAttr != Double.PositiveInfinity
      if (!(isSrcMarked && isDstMarked)) {
        if (isSrcMarked) {
          iter = Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          iter = Iterator((triplet.srcId, triplet.dstAttr + 1))
        }
      }
      iter
    }

    val mergeMsg = { (a: Double, b: Double) =>
      math.min(a, b)
    }

    initialGraph.pregel(Double.PositiveInfinity, maxIter)(vprog, sendMsg, mergeMsg);
  }
}
