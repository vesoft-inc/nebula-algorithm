/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.utils

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.slf4j.LoggerFactory

object NebulaUtil {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * construct original graph
    *
    * @param hasWeight if the graph has no weight, then edge's weight is default 1.0
    * @return Graph
    */
  def loadInitGraph(dataSet: Dataset[Row], hasWeight: Boolean): Graph[None.type, Double] = {
    implicit val encoder: Encoder[Edge[Double]] = org.apache.spark.sql.Encoders.kryo[Edge[Double]]
    val edges: RDD[Edge[Double]] = dataSet.map { row =>
      val attr = if (hasWeight) row.get(2).toString.toDouble else 1.0
      Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, attr)
    }(encoder).rdd

    Graph.fromEdges(edges, None)
  }

  /**
    * Assembly algorithm's result file path
    *
    * @param path algorithm configuration
    * @param algorithmName
    *
    * @return validate result path
    */
  def getResultPath(path: String, algorithmName: String): String =
    if (path.endsWith("/")) s"$path$algorithmName"
    else s"$path/$algorithmName"
}
