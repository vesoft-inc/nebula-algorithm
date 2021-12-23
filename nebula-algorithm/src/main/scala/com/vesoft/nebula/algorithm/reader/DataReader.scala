/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.reader

import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import com.vesoft.nebula.algorithm.config.Configs
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

abstract class DataReader(spark: SparkSession, configs: Configs) {
  def read(): DataFrame
}

class NebulaReader(spark: SparkSession, configs: Configs, partitionNum: String)
    extends DataReader(spark, configs) {
  override def read(): DataFrame = {
    val metaAddress = configs.nebulaConfig.readConfigEntry.address
    val space       = configs.nebulaConfig.readConfigEntry.space
    val labels      = configs.nebulaConfig.readConfigEntry.labels
    val weights     = configs.nebulaConfig.readConfigEntry.weightCols
    val partition   = partitionNum.toInt

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withConenctionRetry(2)
        .build()

    val noColumn = weights.isEmpty

    var dataset: DataFrame = null
    for (i <- labels.indices) {
      val returnCols: ListBuffer[String] = new ListBuffer[String]
      if (configs.dataSourceSinkEntry.hasWeight && weights.nonEmpty) {
        returnCols.append(weights(i))
      }
      val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
        .builder()
        .withSpace(space)
        .withLabel(labels(i))
        .withNoColumn(noColumn)
        .withReturnCols(returnCols.toList)
        .withPartitionNum(partition)
        .build()
      if (dataset == null) {
        dataset = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
        if (weights.nonEmpty) {
          dataset = dataset.select("_srcId", "_dstId", weights(i))
        }
      } else {
        var df = spark.read
          .nebula(config, nebulaReadEdgeConfig)
          .loadEdgesToDF()
        if (weights.nonEmpty) {
          df = df.select("_srcId", "_dstId", weights(i))
        }
        dataset = dataset.union(df)
      }
    }
    dataset
  }
}

class CsvReader(spark: SparkSession, configs: Configs, partitionNum: String)
    extends DataReader(spark, configs) {
  override def read(): DataFrame = {
    val delimiter = configs.localConfigEntry.delimiter
    val header    = configs.localConfigEntry.header
    val localPath = configs.localConfigEntry.filePath

    val partition = partitionNum.toInt

    val data =
      spark.read
        .option("header", header)
        .option("delimiter", delimiter)
        .csv(localPath)
    val weight = configs.localConfigEntry.weight
    val src    = configs.localConfigEntry.srcId
    val dst    = configs.localConfigEntry.dstId
    if (configs.dataSourceSinkEntry.hasWeight && weight != null && !weight.trim.isEmpty) {
      data.select(src, dst, weight)
    } else {
      data.select(src, dst)
    }
    if (partition != 0) {
      data.repartition(partition)
    }
    data
  }
}

class JsonReader(spark: SparkSession, configs: Configs, partitionNum: String)
    extends DataReader(spark, configs) {
  override def read(): DataFrame = {
    val localPath = configs.localConfigEntry.filePath
    val data      = spark.read.json(localPath)
    val partition = partitionNum.toInt

    val weight = configs.localConfigEntry.weight
    val src    = configs.localConfigEntry.srcId
    val dst    = configs.localConfigEntry.dstId
    if (configs.dataSourceSinkEntry.hasWeight && weight != null && !weight.trim.isEmpty) {
      data.select(src, dst, weight)
    } else {
      data.select(src, dst)
    }
    if (partition != 0) {
      data.repartition(partition)
    }
    data
  }
}
