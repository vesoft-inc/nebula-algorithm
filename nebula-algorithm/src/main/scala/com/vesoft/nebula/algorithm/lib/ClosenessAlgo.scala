package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, ClosenessConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.log4j.Logger
import org.apache.spark.graphx
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ClosenessAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)
  val ALGORITHM: String = "Closeness"

  /**
   * run the Closeness algorithm for nebula graph
   */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            closenessConfig: ClosenessConfig,
            hasWeight:Boolean):DataFrame={
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)
    val closeness = execute(graph, closenessConfig.sourceId)
    val algoResult=spark.createDataFrame(Seq((closenessConfig.sourceId,closeness)))
    algoResult
  }

  /**
   * execute Closeness algorithm
   */
  def execute(graph: Graph[None.type, Double],
              sourceId: graphx.VertexId)={
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    val distancesNumAndSum=sssp.vertices.map(a=>(1,a._2)).reduce((a,b)=>(a._1+b._1,a._2+b._2))
    distancesNumAndSum._1/distancesNumAndSum._2
  }
}
