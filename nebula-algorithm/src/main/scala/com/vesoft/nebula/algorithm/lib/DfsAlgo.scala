/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.AlgoConstants.{
  ALGO_ID_COL,
  DFS_RESULT_COL,
  ENCODE_ID_COL,
  ORIGIN_ID_COL
}
import com.vesoft.nebula.algorithm.config.{AlgoConstants, BfsConfig, DfsConfig}
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.collection.mutable

object DfsAlgo {
  var iterNums = 0
  val ALGORITHM = "dfs"

  def apply(spark: SparkSession, dataset: Dataset[Row], dfsConfig: DfsConfig): DataFrame = {
    spark.sparkContext.setJobGroup(ALGORITHM, s"Running $ALGORITHM")

    var encodeIdDf: DataFrame = null
    var finalRoot: Long       = 0

    val graph: Graph[None.type, Double] = if (dfsConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, false)
      encodeIdDf = encodeId
      finalRoot = encodeIdDf.filter(row => row.get(0).toString == dfsConfig.root).first().getLong(1)
      NebulaUtil.loadInitGraph(data, false)
    } else {
      finalRoot = dfsConfig.root.toLong
      NebulaUtil.loadInitGraph(dataset, false)
    }
    val bfsVertices =
      dfs(graph, finalRoot, mutable.Seq.empty[VertexId])(dfsConfig.maxIter).vertices.filter(v =>
        v._2 != Double.PositiveInfinity)

    val schema = StructType(
      List(StructField(ALGO_ID_COL, LongType, nullable = false),
           StructField(DFS_RESULT_COL, DoubleType, nullable = true)))

    val resultRDD = bfsVertices.map(v => Row(v._1, v._2))
    val algoResult =
      spark.sqlContext.createDataFrame(resultRDD, schema).orderBy(col(DFS_RESULT_COL))

    if (dfsConfig.encodeId) {
      DecodeUtil.convertAlgoId2StringId(algoResult, encodeIdDf).coalesce(1)
    } else {
      algoResult.coalesce(1)
    }
  }

  def dfs(g: Graph[None.type, Double], vertexId: VertexId, visited: mutable.Seq[VertexId])(
      maxIter: Int): Graph[Double, Double] = {

    val initialGraph =
      g.mapVertices((id, _) => if (id == vertexId) 0.0 else Double.PositiveInfinity)

    def vertexProgram(id: VertexId, attr: Double, msg: Double): Double = {
      math.min(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
      val sourceVertex = edge.srcAttr
      val targetVertex = edge.dstAttr
      if (sourceVertex + 1 < targetVertex && sourceVertex < maxIter) {
        Iterator((edge.dstId, sourceVertex + 1))
      } else {
        Iterator.empty
      }
    }

    def mergeMessage(a: Double, b: Double): Double = {
      math.min(a, b)
    }

    //开始迭代
    val resultGraph =
      Pregel(initialGraph, Double.PositiveInfinity)(vertexProgram, sendMessage, mergeMessage)

    resultGraph

  }

}
