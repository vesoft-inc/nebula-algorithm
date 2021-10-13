/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.config

import com.vesoft.nebula.algorithm.config.Configs.Argument
import org.junit.Test

import scala.collection.mutable.ListBuffer

class ConfigSuite {

  var configs: Configs = _

  @Test
  def getConfigsSuite(): Unit = {
    val args: ListBuffer[String] = new ListBuffer[String]
    args.append("-p")
    args.append("src/test/resources/application.conf")
    try {
      val options = Configs.parser(args.toArray, "TestProgram")
      val p: Argument = options match {
        case Some(config) => config
        case _ =>
          assert(false)
          sys.exit(-1)
      }
      configs = Configs.parse(p.config)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        assert(false)
      }
    }

  }

  @Test
  def getSparkConfigSuite(): Unit = {
    if (configs == null) {
      getConfigsSuite()
    }
    val sparkConfig = configs.sparkConfig
    assert(sparkConfig.map.size == 3)

    val spark = SparkConfig.getSpark(configs)
    assert(spark.partitionNum.toInt == 100)
  }

  @Test
  def getSourceSinkConfigSuite(): Unit = {
    if (configs == null) {
      getConfigsSuite()
    }
    val dataSourceSinkEntry = configs.dataSourceSinkEntry
    assert(dataSourceSinkEntry.source.equals("csv"))
    assert(dataSourceSinkEntry.sink.equals("nebula"))
    assert(!dataSourceSinkEntry.hasWeight)
  }
  @Test
  def getNebulaConfigSuite(): Unit = {
    if (configs == null) {
      getConfigsSuite()
    }
    val nebulaConfigEntry = configs.nebulaConfig
    val writeConfig       = nebulaConfigEntry.writeConfigEntry
    assert(writeConfig.graphAddress.equals("127.0.0.1:9669"))
    assert(writeConfig.metaAddress.equals("127.0.0.1:9559,127.0.0.1:9560"))
    assert(writeConfig.space.equals("nb"))
    assert(writeConfig.tag.equals("pagerank"))
    assert(writeConfig.user.equals("root"))
    assert(writeConfig.pswd.equals("nebula"))

    val readConfig = nebulaConfigEntry.readConfigEntry
    assert(readConfig.address.equals("127.0.0.1:9559"))
    assert(readConfig.space.equals("nb"))
    assert(readConfig.labels.size == 1)
    assert(readConfig.weightCols.size == 1)
  }

  @Test
  def getLocalConfigSuite(): Unit = {
    if (configs == null) {
      getConfigsSuite()
    }
    val localConfigEntry = configs.localConfigEntry
    assert(localConfigEntry.filePath.startsWith("hdfs://"))
    assert(localConfigEntry.srcId.equals("_c0"))
    assert(localConfigEntry.dstId.equals("_c1"))
    assert(localConfigEntry.weight == null)
    assert(!localConfigEntry.header)
    assert(localConfigEntry.delimiter.equals(","))
    assert(localConfigEntry.resultPath.equals("/tmp/"))
  }

  @Test
  def getAlgoConfigSuite(): Unit = {
    if (configs == null) {
      getConfigsSuite()
    }
    val algoConfig = configs.algorithmConfig
    val algoName   = AlgoConfig.getAlgoName(configs)
    assert(algoName.equals("pagerank"))

    val prConfig = PRConfig.getPRConfig(configs)
    assert(prConfig.maxIter == 10)
    assert(prConfig.resetProb < 0.150000001)

    val louvainConfig = LouvainConfig.getLouvainConfig(configs)
    assert(louvainConfig.maxIter == 20)
    assert(louvainConfig.internalIter == 10)
    assert(louvainConfig.tol < 0.5000001)

    val ccConfig = CcConfig.getCcConfig(configs)
    assert(ccConfig.maxIter == 20)

    val lpaConfig = LPAConfig.getLPAConfig(configs)
    assert(lpaConfig.maxIter == 20)

    val shortestPathConfig = ShortestPathConfig.getShortestPathConfig(configs)
    assert(shortestPathConfig.landmarks.size == 1)

    val kcoreConfig = KCoreConfig.getKCoreConfig(configs)
    assert(kcoreConfig.maxIter == 10)
    assert(kcoreConfig.degree == 1)

    val betweennessConfig = BetweennessConfig.getBetweennessConfig(configs)
    assert(betweennessConfig.maxIter == 5)
  }
}
