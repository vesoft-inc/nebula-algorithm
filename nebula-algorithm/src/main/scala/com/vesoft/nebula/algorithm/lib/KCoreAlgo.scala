/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, KCoreConfig}
import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object KCoreAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "KCore"

  /**
    * run the louvain algorithm for nebula graph
    */
  def apply(spark: SparkSession, dataset: Dataset[Row], kCoreConfig: KCoreConfig): DataFrame = {

    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, false)
    val kCoreGraph                      = execute(graph, kCoreConfig.maxIter, kCoreConfig.degree)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.KCORE_RESULT_COL, IntegerType, nullable = true)
      ))
    val resultRDD  = kCoreGraph.vertices.map(vertex => Row(vertex._1, vertex._2))
    val algoResult = spark.sqlContext.createDataFrame(resultRDD, schema)
    algoResult
  }

  /**
    * extract k-core sub-graph
    */
  def execute(graph: Graph[None.type, Double], maxIter: Int, k: Int): Graph[Int, Double] = {
    var lastVertexNum: Long    = graph.numVertices
    var currentVertexNum: Long = -1
    var isStable: Boolean      = false
    var iterNum: Int           = 0

    var degreeGraph = graph
      .outerJoinVertices(graph.degrees) { (vid, vd, degree) =>
        degree.getOrElse(0)
      }
      .cache
    var subGraph: Graph[Int, Double] = null

    while (iterNum < maxIter) {
      subGraph = degreeGraph.subgraph(vpred = (vid, degree) => degree >= k)
      degreeGraph = subGraph
        .outerJoinVertices(subGraph.degrees) { (vid, vd, degree) =>
          degree.getOrElse(0)
        }
        .cache

      currentVertexNum = degreeGraph.numVertices
      if (currentVertexNum == lastVertexNum) {
        isStable = true;
      } else {
        lastVertexNum = currentVertexNum
      }

      iterNum += 1
    }
    subGraph
  }
}
