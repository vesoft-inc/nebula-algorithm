/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm

import com.vesoft.nebula.algorithm.config.Configs.Argument
import com.vesoft.nebula.algorithm.config.{
  AlgoConfig,
  BetweennessConfig,
  BfsConfig,
  CcConfig,
  CoefficientConfig,
  Configs,
  HanpConfig,
  JaccardConfig,
  KCoreConfig,
  LPAConfig,
  LouvainConfig,
  Node2vecConfig,
  PRConfig,
  ShortestPathConfig,
  SparkConfig
}
import com.vesoft.nebula.algorithm.lib.{
  BetweennessCentralityAlgo,
  BfsAlgo,
  ClosenessAlgo,
  ClusteringCoefficientAlgo,
  ConnectedComponentsAlgo,
  DegreeStaticAlgo,
  GraphTriangleCountAlgo,
  HanpAlgo,
  JaccardAlgo,
  KCoreAlgo,
  LabelPropagationAlgo,
  LouvainAlgo,
  Node2vecAlgo,
  PageRankAlgo,
  ShortestPathAlgo,
  StronglyConnectedComponentsAlgo,
  TriangleCountAlgo
}
import com.vesoft.nebula.algorithm.reader.{CsvReader, JsonReader, NebulaReader}
import com.vesoft.nebula.algorithm.writer.{CsvWriter, NebulaWriter, TextWriter}
import org.apache.commons.math3.ode.UnknownParameterException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * This object is the entry of all graph algorithms.
  *
  * How to use this tool to run algorithm:
  *    1. Configure application.conf file.
  *    2. Make sure your environment has installed spark and started spark service.
  *    3. Submit nebula algorithm application using this command:
  *        spark-submit --class com.vesoft.nebula.tools.algorithm.Main /your-jar-path/nebula-algorithm-1.1.0.jar -p /your-application.conf-path/application.conf
  */
object Main {

  private val LOGGER = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "Nebula graphx"
    val options      = Configs.parser(args, PROGRAM_NAME)
    val p: Argument = options match {
      case Some(config) => config
      case _ =>
        LOGGER.error("Argument parse failed")
        sys.exit(-1)
    }
    val configs = Configs.parse(p.config)
    LOGGER.info(s"configs =  ${configs}")

    val algoName: String = AlgoConfig.getAlgoName(configs)
    LOGGER.info(s"algoName= ${algoName}")

    val sparkConfig  = SparkConfig.getSpark(configs)
    val partitionNum = sparkConfig.partitionNum

    val startTime = System.currentTimeMillis()
    // reader
    val dataSet  = createDataSource(sparkConfig.spark, configs, partitionNum)
    val readTime = System.currentTimeMillis()

    // algorithm
    val algoResult = executeAlgorithm(sparkConfig.spark, algoName, configs, dataSet)
    val algoTime   = System.currentTimeMillis()

    // writer
    saveAlgoResult(algoResult, configs)
    val endTime = System.currentTimeMillis()

    sparkConfig.spark.stop()
    val readDuration  = ((readTime - startTime) / 1000.0).formatted("%.4f")
    val algoDuration  = ((algoTime - readTime) / 1000.0).formatted("%.4f")
    val writeDuration = ((endTime - algoTime) / 1000.0).formatted("%.4f")
    LOGGER.info(
      s"read data source cost: $readDuration s, algo cost: $algoDuration s, write algo result cost: $writeDuration s")
    sys.exit(0)
  }

  /**
    * create data from datasource
    *
    * @param spark
    * @param configs
    * @return DataFrame
    */
  private[this] def createDataSource(spark: SparkSession,
                                     configs: Configs,
                                     partitionNum: String): DataFrame = {
    val dataSource = configs.dataSourceSinkEntry.source
    val dataSet: Dataset[Row] = dataSource.toLowerCase match {
      case "nebula" => {
        val reader = new NebulaReader(spark, configs, partitionNum)
        reader.read()
      }
      case "csv" => {
        val reader = new CsvReader(spark, configs, partitionNum)
        reader.read()
      }
      case "json" => {
        val reader = new JsonReader(spark, configs, partitionNum)
        reader.read()
      }
    }
    dataSet
  }

  /**
    * execute algorithms
    * @param spark
    * @param algoName
    * @param configs
    * @param dataSet
    * @return DataFrame
    */
  private[this] def executeAlgorithm(spark: SparkSession,
                                     algoName: String,
                                     configs: Configs,
                                     dataSet: DataFrame): DataFrame = {
    val hasWeight = configs.dataSourceSinkEntry.hasWeight
    val algoResult = {
      algoName.toLowerCase match {
        case "pagerank" => {
          val pageRankConfig = PRConfig.getPRConfig(configs)
          PageRankAlgo(spark, dataSet, pageRankConfig, hasWeight)
        }
        case "louvain" => {
          val louvainConfig = LouvainConfig.getLouvainConfig(configs)
          LouvainAlgo(spark, dataSet, louvainConfig, hasWeight)
        }
        case "connectedcomponent" => {
          val ccConfig = CcConfig.getCcConfig(configs)
          ConnectedComponentsAlgo(spark, dataSet, ccConfig, hasWeight)
        }
        case "labelpropagation" => {
          val lpaConfig = LPAConfig.getLPAConfig(configs)
          LabelPropagationAlgo(spark, dataSet, lpaConfig, hasWeight)
        }
        case "shortestpaths" => {
          val spConfig = ShortestPathConfig.getShortestPathConfig(configs)
          ShortestPathAlgo(spark, dataSet, spConfig, hasWeight)
        }
        case "degreestatic" => {
          DegreeStaticAlgo(spark, dataSet)
        }
        case "kcore" => {
          val kCoreConfig = KCoreConfig.getKCoreConfig(configs)
          KCoreAlgo(spark, dataSet, kCoreConfig)
        }
        case "stronglyconnectedcomponent" => {
          val ccConfig = CcConfig.getCcConfig(configs)
          StronglyConnectedComponentsAlgo(spark, dataSet, ccConfig, hasWeight)
        }
        case "betweenness" => {
          val betweennessConfig = BetweennessConfig.getBetweennessConfig(configs)
          BetweennessCentralityAlgo(spark, dataSet, betweennessConfig, hasWeight)
        }
        case "trianglecount" => {
          TriangleCountAlgo(spark, dataSet)
        }
        case "graphtrianglecount" => {
          GraphTriangleCountAlgo(spark, dataSet)
        }
        case "clusteringcoefficient" => {
          val coefficientConfig = CoefficientConfig.getCoefficientConfig(configs)
          ClusteringCoefficientAlgo(spark, dataSet, coefficientConfig)
        }
        case "closeness" => {
          ClosenessAlgo(spark, dataSet, hasWeight)
        }
        case "hanp" => {
          val hanpConfig = HanpConfig.getHanpConfig(configs)
          HanpAlgo(spark, dataSet, hanpConfig, hasWeight)
        }
        case "node2vec" => {
          val node2vecConfig = Node2vecConfig.getNode2vecConfig(configs)
          Node2vecAlgo(spark, dataSet, node2vecConfig, hasWeight)
        }
        case "bfs" => {
          val bfsConfig = BfsConfig.getBfsConfig(configs)
          BfsAlgo(spark, dataSet, bfsConfig)
        }
        case "jaccard" => {
          val jaccardConfig = JaccardConfig.getJaccardConfig(configs)
          JaccardAlgo(spark, dataSet, jaccardConfig)
        }
        case _ => throw new UnknownParameterException("unknown executeAlgo name.")
      }
    }
    algoResult
  }

  private[this] def saveAlgoResult(algoResult: DataFrame, configs: Configs): Unit = {
    val dataSink = configs.dataSourceSinkEntry.sink
    dataSink.toLowerCase match {
      case "nebula" => {
        val writer = new NebulaWriter(algoResult, configs)
        writer.write()
      }
      case "csv" => {
        val writer = new CsvWriter(algoResult, configs)
        writer.write()
      }
      case "text" => {
        val writer = new TextWriter(algoResult, configs)
        writer.write()
      }
      case _ => throw new UnsupportedOperationException("unsupported data sink")
    }
  }
}
