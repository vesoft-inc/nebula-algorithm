/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.writer

import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteNebulaVertexConfig}
import com.vesoft.nebula.algorithm.config.Configs
import org.apache.spark.sql.DataFrame

abstract class AlgoWriter(data: DataFrame, configs: Configs) {
  def write(): Unit
}

class NebulaWriter(data: DataFrame, configs: Configs) extends AlgoWriter(data, configs) {
  override def write(): Unit = {
    val graphAddress = configs.nebulaConfig.writeConfigEntry.graphAddress
    val metaAddress  = configs.nebulaConfig.writeConfigEntry.metaAddress
    val space        = configs.nebulaConfig.writeConfigEntry.space
    val tag          = configs.nebulaConfig.writeConfigEntry.tag
    val user         = configs.nebulaConfig.writeConfigEntry.user
    val passwd       = configs.nebulaConfig.writeConfigEntry.pswd

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withGraphAddress(graphAddress)
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace(space)
      .withTag(tag)
      .withVidField("_id")
      .withVidAsProp(false)
      .withBatch(1000)
      .build()
    data.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }
}

class CsvWriter(data: DataFrame, configs: Configs) extends AlgoWriter(data, configs) {
  override def write(): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.repartition(1).write.option("header", true).csv(resultPath)
  }
}

class TextWriter(data: DataFrame, configs: Configs) extends AlgoWriter(data, configs) {
  override def write(): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.repartition(1).write.option("header", true).text(resultPath)
  }
}
