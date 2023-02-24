/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.collection.mutable

object DeepQueryTest {
  private val LOGGER = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val iter = args(0).toInt
    val id   = args(1).toInt

    query(spark, iter, id)
  }

  def readNebulaData(spark: SparkSession): DataFrame = {

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("192.168.15.5:9559")
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("twitter")
      .withLabel("FOLLOW")
      .withNoColumn(true)
      .withLimit(20000)
      .withPartitionNum(120)
      .build()
    val df: DataFrame =
      spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    df
  }

  def deepQuery(df: DataFrame,
                maxIterations: Int,
                startId: Int): Graph[mutable.HashSet[Int], Double] = {
    implicit val encoder: Encoder[Edge[Double]] = org.apache.spark.sql.Encoders.kryo[Edge[Double]]
    val edges: RDD[Edge[Double]] = df
      .map(row => {
        Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, 1.0)
      })(encoder)
      .rdd

    val graph = Graph.fromEdges(edges, None)

    val queryGraph = graph.mapVertices { (vid, _) =>
      mutable.HashSet[Int](vid.toInt)
    }
    queryGraph.cache()
    queryGraph.numVertices
    queryGraph.numEdges
    df.unpersist()

    def sendMessage(edge: EdgeTriplet[mutable.HashSet[Int], Double])
      : Iterator[(VertexId, mutable.HashSet[Int])] = {
      val (smallSet, largeSet) = if (edge.srcAttr.size < edge.dstAttr.size) {
        (edge.srcAttr, edge.dstAttr)
      } else {
        (edge.dstAttr, edge.srcAttr)
      }

      if (smallSet.size == maxIterations) {
        Iterator.empty
      } else {
        val newNeighbors =
          (for (id <- smallSet; neighbor <- largeSet if neighbor != id) yield neighbor)
        Iterator((edge.dstId, newNeighbors))
      }
    }

    val initialMessage = mutable.HashSet[Int]()

    val pregelGraph = Pregel(queryGraph, initialMessage, maxIterations, EdgeDirection.Both)(
      vprog = (id, attr, msg) => attr ++ msg,
      sendMsg = sendMessage,
      mergeMsg = (a, b) => {
        val setResult = a ++ b
        setResult
      }
    )
    pregelGraph.cache()
    pregelGraph.numVertices
    pregelGraph.numEdges
    queryGraph.unpersist()
    pregelGraph
  }

  def query(spark: SparkSession, maxIter: Int, startId: Int): Unit = {
    val start = System.currentTimeMillis()
    val df    = readNebulaData(spark)
    df.cache()
    df.count()
    println(s"read data cost time ${(System.currentTimeMillis() - start)}")

    val startQuery = System.currentTimeMillis()
    val graph      = deepQuery(df, maxIter, startId)

    val endQuery = System.currentTimeMillis()
    val num      = graph.vertices.filter(row => row._2.contains(startId)).count()
    val end      = System.currentTimeMillis()
    println(s"query cost: ${endQuery - startQuery}")
    println(s"count: ${num}, cost: ${end - endQuery}")
  }
}
