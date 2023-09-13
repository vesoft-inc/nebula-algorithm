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

abstract class DataReader {
  val tpe: ReaderType
  def read(spark: SparkSession, configs: Configs, partitionNum: String): DataFrame
}
object DataReader {
  def make(configs: Configs): DataReader = {
    ReaderType.mapping
      .get(configs.dataSourceSinkEntry.source.toLowerCase)
      .collect {
        case ReaderType.json       => new JsonReader
        case ReaderType.nebulaNgql => new NebulaNgqlReader
        case ReaderType.nebula     => new NebulaReader
        case ReaderType.csv        => new CsvReader
      }
      .getOrElse(throw new UnsupportedOperationException("unsupported reader"))
  }
}

class NebulaReader extends DataReader {
  override val tpe: ReaderType = ReaderType.nebula
  override def read(spark: SparkSession, configs: Configs, partitionNum: String): DataFrame = {
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
final class NebulaNgqlReader extends NebulaReader {

  override val tpe: ReaderType = ReaderType.nebulaNgql

  override def read(spark: SparkSession, configs: Configs, partitionNum: String): DataFrame = {
    val metaAddress  = configs.nebulaConfig.readConfigEntry.address
    val graphAddress = configs.nebulaConfig.readConfigEntry.graphAddress
    val space        = configs.nebulaConfig.readConfigEntry.space
    val labels       = configs.nebulaConfig.readConfigEntry.labels
    val weights      = configs.nebulaConfig.readConfigEntry.weightCols
    val partition    = partitionNum.toInt
    val ngql         = configs.nebulaConfig.readConfigEntry.ngql

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withGraphAddress(graphAddress)
        .withConenctionRetry(2)
        .build()

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
        .withPartitionNum(partition)
        .withNgql(ngql)
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

final class CsvReader extends DataReader {
  override val tpe: ReaderType = ReaderType.csv
  override def read(spark: SparkSession, configs: Configs, partitionNum: String): DataFrame = {
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
    if (configs.dataSourceSinkEntry.hasWeight && weight != null && weight.trim.nonEmpty) {
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
final class JsonReader extends DataReader {
  override val tpe: ReaderType = ReaderType.json
  override def read(spark: SparkSession, configs: Configs, partitionNum: String): DataFrame = {
    val localPath = configs.localConfigEntry.filePath
    val data      = spark.read.json(localPath)
    val partition = partitionNum.toInt

    val weight = configs.localConfigEntry.weight
    val src    = configs.localConfigEntry.srcId
    val dst    = configs.localConfigEntry.dstId
    if (configs.dataSourceSinkEntry.hasWeight && weight != null && weight.trim.nonEmpty) {
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
