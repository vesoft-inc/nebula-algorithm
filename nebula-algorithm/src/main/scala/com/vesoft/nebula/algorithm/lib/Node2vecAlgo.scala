/*
 * Copyright (c) 2021. vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, Node2vecConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.Serializable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                    var path: Array[Long] = Array.empty[Long])
    extends Serializable

case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
                    var J: Array[Int] = Array.empty[Int],
                    var q: Array[Double] = Array.empty[Double])
    extends Serializable

/**
  * The implementation of the algorithm refers to the code implemented by the author of the paper.
  * Here is the code url https://github.com/aditya-grover/node2vec.git.
  */
object Node2vecAlgo {
  val ALGORITHM: String                               = "Node2vec"
  var node2vecConfig: Node2vecConfig                  = _
  var context: SparkContext                           = _
  var indexedEdges: RDD[Edge[EdgeAttr]]               = _
  var indexedNodes: RDD[(VertexId, NodeAttr)]         = _
  var graph: Graph[NodeAttr, EdgeAttr]                = _
  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = null
  var vectors: Map[String, Array[Float]]              = _
  lazy val createUndirectedEdge = (srcId: Long, dstId: Long, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight))),
      (dstId, Array((srcId, weight)))
    )
  }
  lazy val createDirectedEdge = (srcId: Long, dstId: Long, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight)))
    )
  }

  def setupAlias(nodeWeights: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val K = nodeWeights.length
    val J = Array.fill(K)(0)
    val q = Array.fill(K)(0.0)

    val smaller = new ArrayBuffer[Int]()
    val larger  = new ArrayBuffer[Int]()

    val sum = nodeWeights.map(_._2).sum
    nodeWeights.zipWithIndex.foreach {
      case ((nodeId, weight), i) =>
        q(i) = K * weight / sum
        if (q(i) < 1.0) {
          smaller.append(i)
        } else {
          larger.append(i)
        }
    }

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    (J, q)
  }

  def setupEdgeAlias(p: Double = 1.0, q: Double = 1.0)(
      srcId: Long,
      srcNeighbors: Array[(Long, Double)],
      dstNeighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val neighbors_ = dstNeighbors.map {
      case (dstNeighborId, weight) =>
        var unnormProb = weight / q
        if (srcId == dstNeighborId) unnormProb = weight / p
        else if (srcNeighbors.exists(_._1 == dstNeighborId)) unnormProb = weight

        (dstNeighborId, unnormProb)
    }

    setupAlias(neighbors_)
  }

  def drawAlias(J: Array[Int], q: Array[Double]): Int = {
    val K  = J.length
    val kk = math.floor(math.random * K).toInt

    if (math.random < q(kk)) kk
    else J(kk)
  }

  def load(graph: Graph[None.type, Double]): this.type = {
    val bcMaxDegree = context.broadcast(node2vecConfig.degree)
    val bcEdgeCreator = node2vecConfig.directed match {
      case true  => context.broadcast(createDirectedEdge)
      case false => context.broadcast(createUndirectedEdge)
    }

    indexedNodes = graph.edges
      .flatMap { row =>
        bcEdgeCreator.value.apply(row.srcId, row.dstId, row.attr)
      }
      .reduceByKey(_ ++ _)
      .map {
        case (nodeId, neighbors: Array[(VertexId, Double)]) =>
          var neighbors_ = neighbors
          if (neighbors_.length > bcMaxDegree.value) {
            neighbors_ = neighbors
              .sortWith { case (left, right) => left._2 > right._2 }
              .slice(0, bcMaxDegree.value)
          }

          (nodeId, NodeAttr(neighbors = neighbors_.distinct))
      }
      .repartition(node2vecConfig.dataNumPartition)
      .cache

    indexedEdges = indexedNodes
      .flatMap {
        case (srcId, clickNode) =>
          clickNode.neighbors.map {
            case (dstId, weight) =>
              Edge(srcId, dstId, EdgeAttr())
          }
      }
      .repartition(node2vecConfig.dataNumPartition)
      .cache
    this
  }

  def initTransitionProb(): this.type = {
    val bcP = context.broadcast(node2vecConfig.p)
    val bcQ = context.broadcast(node2vecConfig.q)

    graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr] {
        case (vertexId, clickNode) =>
          val (j, q)        = this.setupAlias(clickNode.neighbors)
          val nextNodeIndex = this.drawAlias(j, q)
          clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)

          clickNode
      }
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        val (j, q) = this.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId,
                                                               edgeTriplet.srcAttr.neighbors,
                                                               edgeTriplet.dstAttr.neighbors)
        edgeTriplet.attr.J = j
        edgeTriplet.attr.q = q
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)

        edgeTriplet.attr
      }
      .cache

    this
  }

  def randomWalk(): this.type = {
    val edge2attr = graph.triplets
      .map { edgeTriplet =>
        (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
      }
      .repartition(node2vecConfig.dataNumPartition)
      .cache
    edge2attr.first

    for (iter <- 0 until node2vecConfig.numWalks) {
      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk = graph.vertices.map {
        case (nodeId, clickNode) =>
          val pathBuffer = new ArrayBuffer[Long]()
          pathBuffer.append(clickNode.path: _*)
          (nodeId, pathBuffer)
      }.cache
      var activeWalks = randomWalk.first
      graph.unpersist(blocking = false)
      graph.edges.unpersist(blocking = false)
      for (walkCount <- 0 until node2vecConfig.walkLength) {
        prevWalk = randomWalk
        randomWalk = randomWalk
          .map {
            case (srcNodeId, pathBuffer) =>
              val prevNodeId    = pathBuffer(pathBuffer.length - 2)
              val currentNodeId = pathBuffer.last

              (s"$prevNodeId$currentNodeId", (srcNodeId, pathBuffer))
          }
          .join(edge2attr)
          .map {
            case (edge, ((srcNodeId, pathBuffer), attr)) =>
              try {
                val nextNodeIndex = this.drawAlias(attr.J, attr.q)
                val nextNodeId    = attr.dstNeighbors(nextNodeIndex)
                pathBuffer.append(nextNodeId)

                (srcNodeId, pathBuffer)
              } catch {
                case e: Exception => throw new RuntimeException(e.getMessage)
              }
          }
          .cache

        activeWalks = randomWalk.first()
        prevWalk.unpersist(blocking = false)
      }

      if (randomWalkPaths != null) {
        val prevRandomWalkPaths = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).cache()
        randomWalkPaths.first
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
    }
    this
  }

  def embedding(): this.type = {
    val randomPaths = randomWalkPaths
      .map {
        case (vertexId, pathBuffer) =>
          Try(pathBuffer.map(_.toString).toIterable).getOrElse(null)
      }
      .filter(_ != null)
    val word2vec = new Word2Vec()
    word2vec
      .setLearningRate(node2vecConfig.lr)
      .setNumIterations(node2vecConfig.maxIter)
      .setNumPartitions(node2vecConfig.modelNumPartition)
      .setVectorSize(node2vecConfig.dim)
      .setWindowSize(node2vecConfig.window)
    val model = word2vec.fit(randomPaths)
    model.save(context, node2vecConfig.modelPath) // use Word2VecModel.load(context, path) to load model
    this.vectors = model.getVectors
    this
  }

  /**
    * run the Node2vec algorithm for nebula graph
    */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            node2vecConfig: Node2vecConfig,
            hasWeight: Boolean): DataFrame = {
    val inputGraph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)
    this.context = spark.sparkContext
    this.node2vecConfig = node2vecConfig
    val node2vecResult: Map[String, Array[Float]] = this
      .load(inputGraph)
      .initTransitionProb()
      .randomWalk()
      .embedding()
      .vectors
    val node2vecRDD: RDD[Row] = this.context
      .parallelize(node2vecResult.toList)
      .map(row => Row(row._1, row._2.mkString(node2vecConfig.embSeparate)))
    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, StringType, nullable = false),
        StructField(AlgoConstants.NODE2VEC_RESULT_COL, StringType, nullable = true)
      ))
    val algoResult = spark.sqlContext.createDataFrame(node2vecRDD, schema)
    algoResult
  }
}
