/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, BfsConfig, DfsConfig}
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.collection.mutable

object DfsAlgo {
  var iterNums = 0

  def apply(spark: SparkSession, dataset: Dataset[Row], dfsConfig: DfsConfig): DataFrame = {
    var encodeIdDf: DataFrame = null

    val graph: Graph[None.type, Double] = if (dfsConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, false)
      encodeIdDf = encodeId
      NebulaUtil.loadInitGraph(data, false)
    } else {
      NebulaUtil.loadInitGraph(dataset, false)
    }
    val bfsVertices = dfs(graph, dfsConfig.root, mutable.Seq.empty[VertexId])(dfsConfig.maxIter)

    val schema = StructType(List(StructField("dfs", LongType, nullable = false)))

    val rdd = spark.sparkContext.parallelize(bfsVertices.toSeq, 1).map(row => Row(row))
    val algoResult = spark.sqlContext
      .createDataFrame(rdd, schema)

    if (dfsConfig.encodeId) {
      DecodeUtil.convertAlgoId2StringId(algoResult, encodeIdDf).coalesce(1)
    } else {
      algoResult.coalesce(1)
    }
  }

  def dfs(g: Graph[None.type, Double], vertexId: VertexId, visited: mutable.Seq[VertexId])(
      maxIter: Int): mutable.Seq[VertexId] = {
    if (visited.contains(vertexId)) {
      visited
    } else {
      if (iterNums > maxIter) {
        return visited
      }
      val newVisited = visited :+ vertexId
      val neighbors  = g.collectNeighbors(EdgeDirection.Out).lookup(vertexId).flatten
      iterNums = iterNums + 1
      neighbors.foldLeft(newVisited)((visited, neighbor) => dfs(g, neighbor._1, visited)(maxIter))
    }
  }

}
