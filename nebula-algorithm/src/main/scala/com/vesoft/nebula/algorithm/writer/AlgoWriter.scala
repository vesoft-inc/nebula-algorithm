/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.writer

import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteMode, WriteNebulaVertexConfig}
import com.vesoft.nebula.algorithm.config.{AlgoConstants, Configs}
import org.apache.spark.sql.DataFrame

abstract class AlgoWriter {
  val tpe:WriterType
  def write(data: DataFrame, configs: Configs): Unit
}
object AlgoWriter {
  def make(configs: Configs): AlgoWriter = {
    WriterType.mapping.get(configs.dataSourceSinkEntry.sink.toLowerCase).collect {
      case WriterType.text => new TextWriter
      case WriterType.nebula => new NebulaWriter
      case WriterType.csv => new CsvWriter
    }.getOrElse(throw new UnsupportedOperationException("unsupported writer"))
    
  }
}

final class NebulaWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.nebula
  override def write(data: DataFrame, configs: Configs): Unit = {
    val graphAddress = configs.nebulaConfig.writeConfigEntry.graphAddress
    val metaAddress  = configs.nebulaConfig.writeConfigEntry.metaAddress
    val space        = configs.nebulaConfig.writeConfigEntry.space
    val tag          = configs.nebulaConfig.writeConfigEntry.tag
    val user         = configs.nebulaConfig.writeConfigEntry.user
    val passwd       = configs.nebulaConfig.writeConfigEntry.pswd
    val writeType    = configs.nebulaConfig.writeConfigEntry.writeType
    val writeMode    = if (writeType.equals("insert")) WriteMode.INSERT else WriteMode.UPDATE

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withGraphAddress(graphAddress)
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withUser(user)
      .withPasswd(passwd)
      .withSpace(space)
      .withTag(tag)
      .withVidField(AlgoConstants.ALGO_ID_COL)
      .withVidAsProp(false)
      .withBatch(500)
      .withWriteMode(writeMode)
      .build()
    data.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }
}

final class CsvWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.csv
  override def write(data: DataFrame, configs: Configs): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.write.option("header", true).csv(resultPath)
  }
}

final class TextWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.text
  override def write(data: DataFrame, configs: Configs): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.write.option("header", true).text(resultPath)
  }
}
