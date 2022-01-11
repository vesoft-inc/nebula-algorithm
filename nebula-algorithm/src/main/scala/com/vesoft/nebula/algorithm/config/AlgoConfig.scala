/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.config

import org.apache.spark.graphx.VertexId

case class PRConfig(maxIter: Int, resetProb: Double)

/**
  * pagerank algorithm configuration
  */
object PRConfig {
  var maxIter: Int      = _
  var resetProb: Double = _

  def getPRConfig(configs: Configs): PRConfig = {
    val prConfig = configs.algorithmConfig.map

    maxIter = prConfig("algorithm.pagerank.maxIter").toInt
    resetProb =
      if (prConfig.contains("algorithm.pagerank.resetProb"))
        prConfig("algorithm.pagerank.resetProb").toDouble
      else 0.15

    PRConfig(maxIter, resetProb)
  }
}

case class LPAConfig(maxIter: Int)

/**
  * labelPropagation algorithm configuration
  */
object LPAConfig {
  var maxIter: Int = _

  def getLPAConfig(configs: Configs): LPAConfig = {
    val lpaConfig = configs.algorithmConfig.map

    maxIter = lpaConfig("algorithm.labelpropagation.maxIter").toInt
    LPAConfig(maxIter)
  }
}

case class CcConfig(maxIter: Int)

/**
  * ConnectedComponect algorithm configuration
  */
object CcConfig {
  var maxIter: Int = _

  def getCcConfig(configs: Configs): CcConfig = {
    val ccConfig = configs.algorithmConfig.map

    maxIter = ccConfig("algorithm.connectedcomponent.maxIter").toInt
    CcConfig(maxIter)
  }
}

case class ShortestPathConfig(landmarks: Seq[VertexId])

/**
  * ConnectedComponect algorithm configuration
  */
object ShortestPathConfig {
  var landmarks: Seq[Long] = _

  def getShortestPathConfig(configs: Configs): ShortestPathConfig = {
    val spConfig = configs.algorithmConfig.map

    landmarks = spConfig("algorithm.shortestpaths.landmarks").split(",").toSeq.map(_.toLong)
    ShortestPathConfig(landmarks)
  }
}

case class LouvainConfig(maxIter: Int, internalIter: Int, tol: Double)

/**
  * louvain algorithm configuration
  */
object LouvainConfig {
  var maxIter: Int      = _
  var internalIter: Int = _
  var tol: Double       = _

  def getLouvainConfig(configs: Configs): LouvainConfig = {
    val louvainConfig = configs.algorithmConfig.map

    maxIter = louvainConfig("algorithm.louvain.maxIter").toInt
    internalIter = louvainConfig("algorithm.louvain.internalIter").toInt
    tol = louvainConfig("algorithm.louvain.tol").toDouble

    LouvainConfig(maxIter, internalIter, tol)
  }
}

/**
  * degree static
  */
case class DegreeStaticConfig(degree: Boolean, inDegree: Boolean, outDegree: Boolean)

object DegreeStaticConfig {
  var degree: Boolean    = false
  var inDegree: Boolean  = false
  var outDegree: Boolean = false

  def getDegreeStaticConfig(configs: Configs): DegreeStaticConfig = {
    val degreeConfig = configs.algorithmConfig.map
    degree = ConfigUtil.getOrElseBoolean(degreeConfig, "algorithm.degreestatic.degree", false)
    inDegree = ConfigUtil.getOrElseBoolean(degreeConfig, "algorithm.degreestatic.indegree", false)
    outDegree = ConfigUtil.getOrElseBoolean(degreeConfig, "algorithm.degreestatic.outdegree", false)
    DegreeStaticConfig(degree, inDegree, outDegree)
  }
}

/**
  * k-core
  */
case class KCoreConfig(maxIter: Int, degree: Int)

object KCoreConfig {
  var maxIter: Int = _
  var degree: Int  = _

  def getKCoreConfig(configs: Configs): KCoreConfig = {
    val kCoreConfig = configs.algorithmConfig.map
    maxIter = kCoreConfig("algorithm.kcore.maxIter").toInt
    degree = kCoreConfig("algorithm.kcore.degree").toInt
    KCoreConfig(maxIter, degree)
  }
}

/**
  * Betweenness
  */
case class BetweennessConfig(maxIter: Int)

object BetweennessConfig {
  var maxIter: Int = _

  def getBetweennessConfig(configs: Configs): BetweennessConfig = {
    val betweennessConfig = configs.algorithmConfig.map
    maxIter = betweennessConfig("algorithm.betweenness.maxIter").toInt
    BetweennessConfig(maxIter)
  }
}

/**
  * ClusterCoefficient
  * algoType has two options: local or global
  */
case class CoefficientConfig(algoType: String)

object CoefficientConfig {
  var algoType: String = _

  def getCoefficientConfig(configs: Configs): CoefficientConfig = {
    val coefficientConfig = configs.algorithmConfig.map
    algoType = coefficientConfig("algorithm.clusteringcoefficient.type")
    assert(algoType.equalsIgnoreCase("local") || algoType.equalsIgnoreCase("global"),
           "ClusteringCoefficient only support local or global type.")
    CoefficientConfig(algoType)
  }
}

/**
  * bfs
  */
case class BfsConfig(maxIter: Int, root: Long)
object BfsConfig {
  var maxIter: Int = _
  var root: Long   = _

  def getBfsConfig(configs: Configs): BfsConfig = {
    val bfsConfig = configs.algorithmConfig.map
    maxIter = bfsConfig("algorithm.bfs.maxIter").toInt
    root = bfsConfig("algorithm.bfs.root").toLong
    BfsConfig(maxIter, root)
  }
}

/**
  * Hanp
  */
case class HanpConfig(hopAttenuation: Double, maxIter: Int, preference: Double)

object HanpConfig {
  var hopAttenuation: Double = _
  var maxIter: Int           = _
  var preference: Double     = 1.0
  def getHanpConfig(configs: Configs): HanpConfig = {
    val hanpConfig = configs.algorithmConfig.map
    hopAttenuation = hanpConfig("algorithm.hanp.hopAttenuation").toDouble
    maxIter = hanpConfig("algorithm.hanp.maxIter").toInt
    preference = hanpConfig("algorithm.hanp.preference").toDouble
    HanpConfig(hopAttenuation, maxIter, preference)
  }
}

/**
  * Node2vec
  */
case class Node2vecConfig(maxIter: Int,
                          lr: Double,
                          dataNumPartition: Int,
                          modelNumPartition: Int,
                          dim: Int,
                          window: Int,
                          walkLength: Int,
                          numWalks: Int,
                          p: Double,
                          q: Double,
                          directed: Boolean,
                          degree: Int,
                          embSeparate: String,
                          modelPath: String)
object Node2vecConfig {
  var maxIter: Int           = _
  var lr: Double             = _
  var dataNumPartition: Int  = _
  var modelNumPartition: Int = _
  var dim: Int               = _
  var window: Int            = _
  var walkLength: Int        = _
  var numWalks: Int          = _
  var p: Double              = _
  var q: Double              = _
  var directed: Boolean      = _
  var degree: Int            = _
  var embSeparate: String    = _
  var modelPath: String      = _
  def getNode2vecConfig(configs: Configs): Node2vecConfig = {
    val node2vecConfig = configs.algorithmConfig.map
    maxIter = node2vecConfig("algorithm.node2vec.maxIter").toInt
    lr = node2vecConfig("algorithm.node2vec.lr").toDouble
    dataNumPartition = node2vecConfig("algorithm.node2vec.dataNumPartition").toInt
    modelNumPartition = node2vecConfig("algorithm.node2vec.modelNumPartition").toInt
    dim = node2vecConfig("algorithm.node2vec.dim").toInt
    window = node2vecConfig("algorithm.node2vec.window").toInt
    walkLength = node2vecConfig("algorithm.node2vec.walkLength").toInt
    numWalks = node2vecConfig("algorithm.node2vec.numWalks").toInt
    p = node2vecConfig("algorithm.node2vec.p").toDouble
    q = node2vecConfig("algorithm.node2vec.q").toDouble
    directed = node2vecConfig("algorithm.node2vec.directed").toBoolean
    degree = node2vecConfig("algorithm.node2vec.degree").toInt
    embSeparate = node2vecConfig("algorithm.node2vec.embSeparate")
    modelPath = node2vecConfig("algorithm.node2vec.modelPath")
    Node2vecConfig(maxIter,
                   lr,
                   dataNumPartition,
                   modelNumPartition,
                   dim,
                   window,
                   walkLength,
                   numWalks,
                   p,
                   q,
                   directed,
                   degree,
                   embSeparate,
                   modelPath)
  }
}

/**
  * Jaccard
  */
case class JaccardConfig(tol: Double)

object JaccardConfig {
  var tol: Double = _
  def getJaccardConfig(configs: Configs): JaccardConfig = {
    val jaccardConfig = configs.algorithmConfig.map
    tol = jaccardConfig("algorithm.jaccard.tol").toDouble
    JaccardConfig(tol)
  }
}

case class AlgoConfig(configs: Configs)

object AlgoConfig {
  def getAlgoName(configs: Configs): String = {
    val algoConfig = configs.algorithmConfig.map
    algoConfig("algorithm.executeAlgo")
  }
}

object ConfigUtil {
  def getOrElseBoolean(config: Map[String, String], key: String, defaultValue: Boolean): Boolean = {
    if (config.contains(key)) {
      config(key).toBoolean
    } else {
      defaultValue
    }
  }

}
