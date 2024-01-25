/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.AlgoConstants
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ClosenessAlgo {
  private val LOGGER    = Logger.getLogger(this.getClass)
  val ALGORITHM: String = "Closeness"
  type SPMap = Map[VertexId, Double]

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def addMap(spmap: SPMap, weight: Double): SPMap = spmap.map {
    case (v, d) => v -> (d + weight)
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
    (spmap1.keySet ++ spmap2.keySet).map { k =>
      k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }(collection.breakOut)
  }

  /**
    * run the Closeness algorithm for nebula graph
    */
  def apply(spark: SparkSession, dataset: Dataset[Row], hasWeight: Boolean): DataFrame = {
    spark.sparkContext.setJobGroup(ALGORITHM, s"Running $ALGORITHM")

    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)
    val closenessRDD                    = execute(graph)
    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.CLOSENESS_RESULT_COL, DoubleType, nullable = true)
      ))
    val algoResult = spark.sqlContext.createDataFrame(closenessRDD, schema)
    algoResult
  }

  /**
    * execute Closeness algorithm
    */
  def execute(graph: Graph[None.type, Double]): RDD[Row] = {
    val spGraph = graph.mapVertices((vid, _) => makeMap(vid -> 0.0))

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = addMap(edge.dstAttr, edge.attr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }
    val spsGraph = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
    val closenessRDD = spsGraph.vertices.map(vertex => {
      var dstNum         = 0
      var dstDistanceSum = 0.0
      for (distance <- vertex._2.values) {
        dstNum += 1
        dstDistanceSum += distance
      }
      Row(vertex._1, (dstNum - 1) / dstDistanceSum)
    })
    closenessRDD
  }
}
