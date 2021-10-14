/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    val edges: RDD[Edge[Double]] = dataSet
      .map(row => {
        if (hasWeight) {
          Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, row.get(2).toString.toDouble)
        } else {
          Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, 1.0)
        }
      })(encoder)
      .rdd

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
  def getResultPath(path: String, algorithmName: String): String = {
    var resultFilePath = path
    if (!resultFilePath.endsWith("/")) {
      resultFilePath = resultFilePath + "/"
    }
    resultFilePath + algorithmName
  }
}
