/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, KCoreConfig, KNeighborsConfig}
import com.vesoft.nebula.algorithm.lib.KCoreAlgo.execute
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KStepNeighbors {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "KStepNeighbors"

  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            kStepConfig: KNeighborsConfig): DataFrame = {
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, false)
    graph.persist()
    graph.numVertices
    graph.numEdges
    dataset.unpersist(blocking = false)

    execute(graph, kStepConfig.steps, kStepConfig.startId)
    null
  }

  def execute(graph: Graph[None.type, Double], steps: List[Int], startId: Long): Unit = {
    val queryGraph = graph.mapVertices { case (vid, _) => vid == startId }

    val initialMessage = false
    def sendMessage(edge: EdgeTriplet[Boolean, Double]): Iterator[(VertexId, Boolean)] = {
      if (edge.srcAttr && !edge.dstAttr)
        Iterator((edge.dstId, true))
      else if (edge.dstAttr && !edge.srcAttr)
        Iterator((edge.srcId, true))
      else
        Iterator.empty
    }

    val costs: ArrayBuffer[Long]  = new ArrayBuffer[Long](steps.size)
    val counts: ArrayBuffer[Long] = new ArrayBuffer[Long](steps.size)

    for (iter <- steps) {
      LOGGER.info(s">>>>>>>>>>>>>>> query ${iter} steps for $startId  >>>>>>>>>>>>>>> ")
      val startQuery = System.currentTimeMillis()
      val pregelGraph = Pregel(queryGraph, initialMessage, iter, EdgeDirection.Either)(
        vprog = (id, attr, msg) => attr | msg,
        sendMsg = sendMessage,
        mergeMsg = (a, b) => a | b
      )
      val endQuery = System.currentTimeMillis()
      val num      = pregelGraph.vertices.filter(row => row._2).count()
      costs.append(endQuery - startQuery)
      counts.append(num)
    }

    val timeCosts   = costs.toArray
    val neighborNum = counts.toArray
    for (i <- 1 to steps.size) {
      print(s"query ${steps(i - 1)} step neighbors cost: ${timeCosts(i - 1)}, ")
      println(s"neighbor number is : ${neighborNum(i - 1)} ")
    }
  }
}
