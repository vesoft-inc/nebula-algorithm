/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.JaccardConfig
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{
  CountVectorizer,
  CountVectorizerModel,
  MinHashLSH,
  MinHashLSHModel
}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object JaccardAlgo {
  private val LOGGER = Logger.getLogger(this.getClass)

  val ALGORITHM: String = "Jaccard"

  /**
    * run the Jaccard algorithm for nebula graph
    */
  def apply(spark: SparkSession, dataset: Dataset[Row], jaccardConfig: JaccardConfig): DataFrame = {

    val jaccardResult: RDD[Row] = execute(spark, dataset, jaccardConfig.tol)

    val schema = StructType(
      List(
        StructField("srcId", StringType, nullable = true),
        StructField("dstId", StringType, nullable = true),
        StructField("similarity", DoubleType, nullable = true)
      ))
    val algoResult = spark.sqlContext.createDataFrame(jaccardResult, schema)
    algoResult
  }

  def execute(spark: SparkSession, dataset: Dataset[Row], tol: Double): RDD[Row] = {
    // compute the node's 1-degree neighbor set
    import spark.implicits._
    val edges = dataset
      .map(row => {
        (row.get(0).toString, row.get(1).toString)
      })
      .rdd

    // get in-degree neighbors
    val inputNodeVector: RDD[(String, List[String])] = edges
      .map(_.swap)
      .combineByKey((v: String) => List(v),
                    (c: List[String], v: String) => v :: c,
                    (c1: List[String], c2: List[String]) => c1 ::: c2)
      .repartition(100)

    // get out-degree neighbors
    val outputNodeVector: RDD[(String, List[String])] = edges
      .combineByKey(
        (v: String) => List(v),
        (c: List[String], v: String) => v :: c,
        (c1: List[String], c2: List[String]) => c1 ::: c2
      )
      .repartition(100)

    // combine the neighbors
    val nodeVector: RDD[(String, List[String])] = inputNodeVector
      .fullOuterJoin(outputNodeVector)
      .map(row => {
        val inNeighbors: Option[List[String]]  = row._2._1
        val outNeighbors: Option[List[String]] = row._2._2
        val neighbors = if (inNeighbors.isEmpty && outNeighbors.isEmpty) {
          (row._1, List())
        } else if (inNeighbors.isEmpty && outNeighbors.isDefined) {
          (row._1, outNeighbors.get)
        } else if (inNeighbors.isDefined && outNeighbors.isEmpty) {
          (row._1, inNeighbors.get)
        } else {
          (row._1, (inNeighbors.get ::: outNeighbors.get).distinct)
        }
        neighbors
      })

    // Preprocess the input data, process it into a 0-1 vector in the form of bag of word
    val inputNodeVectorDF = spark.createDataFrame(nodeVector).toDF("node", "neighbors")
    val cvModel: CountVectorizerModel =
      new CountVectorizer()
        .setInputCol("neighbors")
        .setOutputCol("features")
        .setBinary(true)
        .fit(inputNodeVectorDF)

    val inputNodeVectorDFSparse: DataFrame =
      cvModel.transform(inputNodeVectorDF).select("node", "features")

    val nodeVectorDFSparseFilter = spark
      .createDataFrame(
        inputNodeVectorDFSparse.rdd
          .map(row => (row.getAs[String]("node"), row.getAs[SparseVector]("features")))
          .map(x => (x._1, x._2, x._2.numNonzeros))
          .filter(x => x._3 >= 1)
          .map(x => (x._1, x._2)))
      .toDF("node", "features")

    // call ml's minhashLSH to compute the Jaccard
    val mh                     = new MinHashLSH().setNumHashTables(100).setInputCol("features").setOutputCol("hashes")
    val model: MinHashLSHModel = mh.fit(nodeVectorDFSparseFilter)
    val nodeDistance: DataFrame = model
      .approxSimilarityJoin(nodeVectorDFSparseFilter,
                            nodeVectorDFSparseFilter,
                            tol,
                            "JaccardDistance")
      .select(col("datasetA.node").alias("node1"),
              col("datasetB.node").alias("node2"),
              col("JaccardDistance"))

    val nodeOverlapRatio = nodeDistance.rdd
      .map(x => {
        val node1        = x.getString(0)
        val node2        = x.getString(1)
        val overlapRatio = 1 - x.getDouble(2)
        if (node1 < node2) ((node1, node2), overlapRatio) else ((node2, node1), overlapRatio)
      })
      .filter(x => x._1._1 != x._1._2)
      .map(row => {
        Row(row._1._1, row._1._2, row._2)
      })

    nodeOverlapRatio.distinct()
  }
}
