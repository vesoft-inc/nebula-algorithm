/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, DegreeStaticConfig}
import com.vesoft.nebula.algorithm.utils.{DecodeUtil, NebulaUtil}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

object DegreeStaticAlgo {

  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "DegreeStatic"

  /**
    * run the pagerank algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            degreeConfig: DegreeStaticConfig = new DegreeStaticConfig): DataFrame = {
    var encodeIdDf: DataFrame = null

    val graph: Graph[None.type, Double] = if (degreeConfig.encodeId) {
      val (data, encodeId) = DecodeUtil.convertStringId2LongId(dataset, false)
      encodeIdDf = encodeId
      NebulaUtil.loadInitGraph(data, false)
    } else {
      NebulaUtil.loadInitGraph(dataset, false)
    }

    val degreeResultRDD = execute(graph)

    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.DEGREE_RESULT_COL, IntegerType, nullable = true),
        StructField(AlgoConstants.INDEGREE_RESULT_COL, IntegerType, nullable = true),
        StructField(AlgoConstants.OUTDEGREE_RESULT_COL, IntegerType, nullable = true)
      ))
    val algoResult = spark.sqlContext
      .createDataFrame(degreeResultRDD, schema)

    if (degreeConfig.encodeId) {
      DecodeUtil.convertAlgoId2StringId(algoResult, encodeIdDf)
    } else {
      algoResult
    }
  }

  def execute(graph: Graph[None.type, Double]): RDD[Row] = {
    val degreeRdd: VertexRDD[Int]    = graph.degrees
    val inDegreeRdd: VertexRDD[Int]  = graph.inDegrees
    val outDegreeRdd: VertexRDD[Int] = graph.outDegrees

    val degreeAndInDegree: VertexRDD[(Int, Int)] =
      degreeRdd.leftJoin(inDegreeRdd)((id, d, inD) => (d, inD.getOrElse(0)))

    val result = degreeAndInDegree.leftJoin(outDegreeRdd)((id, dAndInD, opt) =>
      (dAndInD._1, dAndInD._2, opt.getOrElse(0)))
    result.map(vertex => Row(vertex._1, vertex._2._1, vertex._2._2, vertex._2._3))
  }

}
