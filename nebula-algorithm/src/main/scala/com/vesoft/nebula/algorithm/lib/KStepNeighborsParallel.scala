/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.KNeighborsParallelConfig
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KStepNeighborsParallel {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "KStepNeighborsParallel"

  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            kStepConfig: KNeighborsParallelConfig): DataFrame = {
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, false)
    graph.persist()
    graph.numVertices
    graph.numEdges
    dataset.unpersist(blocking = false)

    execute(graph, kStepConfig.steps, kStepConfig.startIds)
    null
  }

  def execute(graph: Graph[None.type, Double], steps: List[Int], startIds: List[Long]): Unit = {
    val queryGraph = graph.mapVertices {
      case (vid, _) =>
        if (startIds.contains(vid)) Map[VertexId, Boolean](vid -> true)
        else Map[VertexId, Boolean]()
    }

    val initialMessage = Map[VertexId, Boolean]()

    def sendMessage(
        edge: EdgeTriplet[Map[Long, Boolean], Double]): Iterator[(VertexId, Map[Long, Boolean])] = {
      if (edge.srcAttr.equals(edge.dstAttr)) {
        Iterator.empty
      } else if (edge.srcAttr.isEmpty) {
        Iterator((edge.srcId, edge.dstAttr))
      } else if (edge.dstAttr.isEmpty) {
        Iterator((edge.dstId, edge.srcAttr))
      } else
        Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr))
    }

    val costs: ArrayBuffer[Long] = new ArrayBuffer[Long](steps.size)
    val nums: ArrayBuffer[mutable.Map[Long, Long]] =
      new ArrayBuffer[mutable.Map[VertexId, VertexId]](steps.size)

    for (iter <- steps) {
      LOGGER.info(s">>>>>>>>>>>>>>> query ${iter} steps for $startIds  >>>>>>>>>>>>>>> ")
      val startQuery = System.currentTimeMillis()
      val pregelGraph = Pregel(queryGraph, initialMessage, iter, EdgeDirection.Either)(
        vprog = (id, attr, msg) => attr ++ msg,
        sendMsg = sendMessage,
        mergeMsg = (a, b) => a ++ b
      )

      val endQuery                              = System.currentTimeMillis()
      val vertexCounts: mutable.Map[Long, Long] = new mutable.HashMap[Long, Long]()
      for (id <- startIds) {
        val num =
          pregelGraph.vertices.filter(row => row._2.contains(id)).count()
        vertexCounts.put(id, num)
      }
      costs.append(endQuery - startQuery)
      nums.append(vertexCounts)
    }

    val timeCosts   = costs.toArray
    val neighborNum = nums.toArray
    for (i <- 1 to steps.size) {
      print(s"query ${steps(i - 1)} step neighbors cost: ${timeCosts(i - 1)}, ")
      println(s"neighbor number is : ${printNeighbors(neighborNum(i - 1))} ")
    }
  }

  def printNeighbors(result: mutable.Map[Long, Long]): String = {
    val sb = new StringBuilder
    for (key <- result.keySet) {
      sb.append(key)
      sb.append(":")
      sb.append(result(key))
      sb.append(";")
    }
    sb.toString.substring(0, sb.length - 1)
  }
}
