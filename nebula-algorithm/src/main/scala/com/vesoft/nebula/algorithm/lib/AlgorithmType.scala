package com.vesoft.nebula.algorithm.lib

/**
  *
  * @author 梦境迷离
  * @version 1.0,2023/9/12
  */
sealed trait AlgorithmType {
  self =>
  def stringify: String = self match {
    case AlgorithmType.Bfs                         => "bfs"
    case AlgorithmType.Closeness                   => "closeness"
    case AlgorithmType.ClusteringCoefficient       => "clusteringcoefficient"
    case AlgorithmType.ConnectedComponents         => "connectedcomponent"
    case AlgorithmType.DegreeStatic                => "degreestatic"
    case AlgorithmType.Dfs                         => "dfs"
    case AlgorithmType.GraphTriangleCount          => "graphtrianglecount"
    case AlgorithmType.Hanp                        => "hanp"
    case AlgorithmType.Jaccard                     => "jaccard"
    case AlgorithmType.KCore                       => "kcore"
    case AlgorithmType.LabelPropagation            => "labelpropagation"
    case AlgorithmType.Louvain                     => "louvain"
    case AlgorithmType.Node2vec                    => "node2vec"
    case AlgorithmType.PageRank                    => "pagerank"
    case AlgorithmType.ShortestPath                => "shortestpaths"
    case AlgorithmType.StronglyConnectedComponents => "stronglyconnectedcomponent"
    case AlgorithmType.TriangleCount               => "trianglecount"
    case AlgorithmType.BetweennessCentrality       => "betweenness"
  }
}
object AlgorithmType {
  lazy val mapping: Map[String, AlgorithmType] = Map(
    Bfs.stringify                         -> Bfs,
    Closeness.stringify                   -> Closeness,
    ClusteringCoefficient.stringify       -> ClusteringCoefficient,
    ConnectedComponents.stringify         -> ConnectedComponents,
    DegreeStatic.stringify                -> DegreeStatic,
    GraphTriangleCount.stringify          -> GraphTriangleCount,
    Hanp.stringify                        -> Hanp,
    Jaccard.stringify                     -> Jaccard,
    KCore.stringify                       -> KCore,
    LabelPropagation.stringify            -> LabelPropagation,
    Louvain.stringify                     -> Louvain,
    Node2vec.stringify                    -> Node2vec,
    PageRank.stringify                    -> PageRank,
    ShortestPath.stringify                -> ShortestPath,
    StronglyConnectedComponents.stringify -> StronglyConnectedComponents,
    TriangleCount.stringify               -> TriangleCount,
    BetweennessCentrality.stringify       -> BetweennessCentrality
  )
  object BetweennessCentrality       extends AlgorithmType
  object Bfs                         extends AlgorithmType
  object Closeness                   extends AlgorithmType
  object ClusteringCoefficient       extends AlgorithmType
  object ConnectedComponents         extends AlgorithmType
  object DegreeStatic                extends AlgorithmType
  object Dfs                         extends AlgorithmType
  object GraphTriangleCount          extends AlgorithmType
  object Hanp                        extends AlgorithmType
  object Jaccard                     extends AlgorithmType
  object KCore                       extends AlgorithmType
  object LabelPropagation            extends AlgorithmType
  object Louvain                     extends AlgorithmType
  object Node2vec                    extends AlgorithmType
  object PageRank                    extends AlgorithmType
  object ShortestPath                extends AlgorithmType
  object StronglyConnectedComponents extends AlgorithmType
  object TriangleCount               extends AlgorithmType
}
