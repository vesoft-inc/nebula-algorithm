/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.config

import com.vesoft.nebula.algorithm.config.JaccardConfig.encodeId
import org.apache.spark.graphx.VertexId

case class PRConfig(maxIter: Int, resetProb: Double, encodeId: Boolean = false)

/**
  * pagerank algorithm configuration
  */
object PRConfig {
  var maxIter: Int      = _
  var resetProb: Double = _
  var encodeId: Boolean = false

  def getPRConfig(configs: Configs): PRConfig = {
    val prConfig = configs.algorithmConfig.map

    maxIter = prConfig("algorithm.pagerank.maxIter").toInt
    resetProb =
      if (prConfig.contains("algorithm.pagerank.resetProb"))
        prConfig("algorithm.pagerank.resetProb").toDouble
      else 0.15
    encodeId = ConfigUtil.getOrElseBoolean(prConfig, "algorithm.pagerank.encodeId")
    PRConfig(maxIter, resetProb, encodeId)
  }
}

case class LPAConfig(maxIter: Int, encodeId: Boolean = false)

/**
  * labelPropagation algorithm configuration
  */
object LPAConfig {
  var maxIter: Int      = _
  var encodeId: Boolean = false

  def getLPAConfig(configs: Configs): LPAConfig = {
    val lpaConfig = configs.algorithmConfig.map

    maxIter = lpaConfig("algorithm.labelpropagation.maxIter").toInt
    encodeId = ConfigUtil.getOrElseBoolean(lpaConfig, "algorithm.labelpropagation.encodeId")
    LPAConfig(maxIter, encodeId)
  }
}

case class CcConfig(maxIter: Int, encodeId: Boolean = false)

/**
  * ConnectedComponect algorithm configuration
  */
object CcConfig {
  var maxIter: Int      = _
  var encodeId: Boolean = false

  def getCcConfig(configs: Configs): CcConfig = {
    val ccConfig = configs.algorithmConfig.map
    encodeId = ConfigUtil.getOrElseBoolean(ccConfig, "algorithm.connectedcomponent.encodeId")

    maxIter = ccConfig("algorithm.connectedcomponent.maxIter").toInt
    CcConfig(maxIter, encodeId)
  }
}

case class ShortestPathConfig(landmarks: Seq[VertexId], encodeId: Boolean = false)

/**
  * ConnectedComponect algorithm configuration
  */
object ShortestPathConfig {
  var landmarks: Seq[Long] = _
  var encodeId: Boolean    = false

  def getShortestPathConfig(configs: Configs): ShortestPathConfig = {
    val spConfig = configs.algorithmConfig.map

    landmarks = spConfig("algorithm.shortestpaths.landmarks").split(",").toSeq.map(_.toLong)
    encodeId = ConfigUtil.getOrElseBoolean(spConfig, "algorithm.shortestpaths.encodeId")
    ShortestPathConfig(landmarks, encodeId)
  }
}

case class LouvainConfig(maxIter: Int, internalIter: Int, tol: Double, encodeId: Boolean = false)

/**
  * louvain algorithm configuration
  */
object LouvainConfig {
  var maxIter: Int      = _
  var internalIter: Int = _
  var tol: Double       = _
  var encodeId: Boolean = false

  def getLouvainConfig(configs: Configs): LouvainConfig = {
    val louvainConfig = configs.algorithmConfig.map

    maxIter = louvainConfig("algorithm.louvain.maxIter").toInt
    internalIter = louvainConfig("algorithm.louvain.internalIter").toInt
    tol = louvainConfig("algorithm.louvain.tol").toDouble
    encodeId = ConfigUtil.getOrElseBoolean(louvainConfig, "algorithm.louvain.encodeId")

    LouvainConfig(maxIter, internalIter, tol, encodeId)
  }
}

/**
  * degree static
  */
case class DegreeStaticConfig(encodeId: Boolean = false)

object DegreeStaticConfig {
  var encodeId: Boolean = false

  def getDegreeStaticConfig(configs: Configs): DegreeStaticConfig = {
    val degreeConfig = configs.algorithmConfig.map
    encodeId = ConfigUtil.getOrElseBoolean(degreeConfig, "algorithm.degreestatic.encodeId")
    DegreeStaticConfig(encodeId)
  }
}

/**
  * graph triangle count
  */
case class TriangleConfig(encodeId: Boolean = false)

object TriangleConfig {
  var encodeId: Boolean = false
  def getTriangleConfig(configs: Configs): TriangleConfig = {
    val triangleConfig = configs.algorithmConfig.map
    encodeId = ConfigUtil.getOrElseBoolean(triangleConfig, "algorithm.trianglecount.encodeId")
    TriangleConfig(encodeId)
  }
}

/**
  * k-core
  */
case class KCoreConfig(maxIter: Int, degree: Int, encodeId: Boolean = false)

object KCoreConfig {
  var maxIter: Int      = _
  var degree: Int       = _
  var encodeId: Boolean = false

  def getKCoreConfig(configs: Configs): KCoreConfig = {
    val kCoreConfig = configs.algorithmConfig.map
    maxIter = kCoreConfig("algorithm.kcore.maxIter").toInt
    degree = kCoreConfig("algorithm.kcore.degree").toInt
    encodeId = ConfigUtil.getOrElseBoolean(kCoreConfig, "algorithm.kcore.encodeId")
    KCoreConfig(maxIter, degree, false)
  }
}

/**
  * Betweenness
  */
case class BetweennessConfig(maxIter: Int, encodeId: Boolean = false)

object BetweennessConfig {
  var maxIter: Int      = _
  var encodeId: Boolean = false

  def getBetweennessConfig(configs: Configs): BetweennessConfig = {
    val betweennessConfig = configs.algorithmConfig.map
    maxIter = betweennessConfig("algorithm.betweenness.maxIter").toInt
    encodeId = ConfigUtil.getOrElseBoolean(betweennessConfig, "algorithm.betweenness.encodeId")
    BetweennessConfig(maxIter, encodeId)
  }
}

/**
  * ClusterCoefficient
  * algoType has two options: local or global
  */
case class CoefficientConfig(algoType: String, encodeId: Boolean = false)

object CoefficientConfig {
  var algoType: String  = _
  var encodeId: Boolean = false

  def getCoefficientConfig(configs: Configs): CoefficientConfig = {
    val coefficientConfig = configs.algorithmConfig.map
    algoType = coefficientConfig("algorithm.clusteringcoefficient.type")
    assert(algoType.equalsIgnoreCase("local") || algoType.equalsIgnoreCase("global"),
           "ClusteringCoefficient only support local or global type.")
    encodeId =
      ConfigUtil.getOrElseBoolean(coefficientConfig, "algorithm.clusteringcoefficient.encodeId")
    CoefficientConfig(algoType, encodeId)
  }
}

/**
  * bfs
  */
case class BfsConfig(maxIter: Int, root: String, encodeId: Boolean = false)
object BfsConfig {
  var maxIter: Int      = _
  var root: String      = _
  var encodeId: Boolean = false

  def getBfsConfig(configs: Configs): BfsConfig = {
    val bfsConfig = configs.algorithmConfig.map
    maxIter = bfsConfig("algorithm.bfs.maxIter").toInt
    root = bfsConfig("algorithm.bfs.root").toString
    encodeId = ConfigUtil.getOrElseBoolean(bfsConfig, "algorithm.bfs.encodeId")
    BfsConfig(maxIter, root, encodeId)
  }
}

/**
  * dfs
  */
case class DfsConfig(maxIter: Int, root: String, encodeId: Boolean = false)
object DfsConfig {
  var maxIter: Int      = _
  var root: String      = _
  var encodeId: Boolean = false

  def getDfsConfig(configs: Configs): DfsConfig = {
    val dfsConfig = configs.algorithmConfig.map
    maxIter = dfsConfig("algorithm.dfs.maxIter").toInt
    root = dfsConfig("algorithm.dfs.root").toString
    encodeId = ConfigUtil.getOrElseBoolean(dfsConfig, "algorithm.dfs.encodeId")
    DfsConfig(maxIter, root, encodeId)
  }
}

/**
  * Hanp
  */
case class HanpConfig(hopAttenuation: Double,
                      maxIter: Int,
                      preference: Double,
                      encodeId: Boolean = false)

object HanpConfig {
  var hopAttenuation: Double = _
  var maxIter: Int           = _
  var preference: Double     = 1.0
  var encodeId: Boolean      = false
  def getHanpConfig(configs: Configs): HanpConfig = {
    val hanpConfig = configs.algorithmConfig.map
    hopAttenuation = hanpConfig("algorithm.hanp.hopAttenuation").toDouble
    maxIter = hanpConfig("algorithm.hanp.maxIter").toInt
    preference = hanpConfig("algorithm.hanp.preference").toDouble
    encodeId = ConfigUtil.getOrElseBoolean(hanpConfig, "algorithm.hanp.encodeId")
    HanpConfig(hopAttenuation, maxIter, preference, encodeId)
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
                          modelPath: String,
                          encodeId: Boolean = false)
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
  var encodeId: Boolean      = false
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
    encodeId = ConfigUtil.getOrElseBoolean(node2vecConfig, "algorithm.node2vec.encodeId")
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
                   modelPath,
                   encodeId)
  }
}

/**
  * Jaccard
  */
case class JaccardConfig(tol: Double, encodeId: Boolean = false)

object JaccardConfig {
  var tol: Double       = _
  var encodeId: Boolean = false
  def getJaccardConfig(configs: Configs): JaccardConfig = {
    val jaccardConfig = configs.algorithmConfig.map
    tol = jaccardConfig("algorithm.jaccard.tol").toDouble
    encodeId = ConfigUtil.getOrElseBoolean(jaccardConfig, "algorithm.jaccard.encodeId")
    JaccardConfig(tol, encodeId)
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
  def getOrElseBoolean(config: Map[String, String], key: String): Boolean =
    config.get(key).exists(_.toBoolean)
}
