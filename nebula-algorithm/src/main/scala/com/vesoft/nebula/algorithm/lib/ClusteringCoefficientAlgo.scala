/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, CoefficientConfig, KCoreConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ClusteringCoefficientAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "ClusterCoefficientAlgo"

  /**
    * run the clusterCoefficient algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            coefficientConfig: CoefficientConfig): DataFrame = {

    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, false)
    var algoResult: DataFrame           = null

    if (coefficientConfig.algoType.equalsIgnoreCase("local")) {
      // compute local clustering coefficient
      val localClusterCoefficient = executeLocalCC(graph)
      val schema = StructType(
        List(
          StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
          StructField(AlgoConstants.CLUSTERCOEFFICIENT_RESULT_COL, DoubleType, nullable = true)
        ))
      algoResult = spark.sqlContext.createDataFrame(localClusterCoefficient, schema)
      // print the graph's average clustering coefficient

      import spark.implicits._
      val vertexNum = algoResult.count()

      val averageCoeff: Double =
        if (vertexNum == 0) 0
        else
          algoResult.map(row => row.get(1).toString.toDouble).reduce(_ + _) / algoResult.count()
      LOGGER.info(s"graph's average clustering coefficient is ${averageCoeff}")

    } else {
      // compute global clustering coefficient
      val GlobalClusterCoefficient: Double = executeGlobalCC(graph)
      val list                             = List(GlobalClusterCoefficient)
      val rdd                              = spark.sparkContext.parallelize(list).map(row => Row(row))

      val schema = StructType(
        List(
          StructField("globalClusterCoefficient", DoubleType, nullable = false)
        ))
      algoResult = spark.sqlContext.createDataFrame(rdd, schema)
    }
    algoResult
  }

  /**
    * execute local cluster coefficient
    */
  def executeLocalCC(graph: Graph[None.type, Double]): RDD[Row] = {
    // compute the actual triangle count for each vertex
    val triangleNum = graph.triangleCount().vertices
    // compute the open triangle count for each vertex
    val idealTriangleNum = graph.degrees.mapValues(degree => degree * (degree - 1) / 2)
    val result = triangleNum
      .innerJoin(idealTriangleNum) { (vid, actualCount, idealCount) =>
        {
          if (idealCount == 0) 0.0
          else (actualCount / idealCount * 1.0).formatted("%.6f").toDouble
        }
      }
      .map(vertex => Row(vertex._1, vertex._2))

    result
  }

  /**
    * execute global cluster coefficient
    */
  def executeGlobalCC(graph: Graph[None.type, Double]): Double = {
    // compute the number of closed triangle
    val closedTriangleNum = graph.triangleCount().vertices.map(_._2).reduce(_ + _)
    // compute the number of open triangle and closed triangle (According to C(n,2)=n*(n-1)/2)
    val triangleNum = graph.degrees.map(vertex => (vertex._2 * (vertex._2 - 1)) / 2.0).reduce(_ + _)
    if (triangleNum == 0)
      0.0
    else
      (closedTriangleNum / triangleNum * 1.0).formatted("%.6f").toDouble
  }
}
