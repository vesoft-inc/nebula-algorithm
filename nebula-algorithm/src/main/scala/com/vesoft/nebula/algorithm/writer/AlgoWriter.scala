/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.writer

import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteMode, WriteNebulaVertexConfig}
import com.vesoft.nebula.algorithm.config.{AlgoConstants, Configs}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

abstract class AlgoWriter {
  val tpe:WriterType
  def write(spark: SparkSession, data: DataFrame, configs: Configs): Unit
}
object AlgoWriter {
  def make(configs: Configs): AlgoWriter = {
    WriterType.mapping.get(configs.dataSourceSinkEntry.sink.toLowerCase).collect {
      case WriterType.text => new TextWriter
      case WriterType.nebula => new NebulaWriter
      case WriterType.csv => new CsvWriter
      case WriterType.hive => new HiveWriter
    }.getOrElse(throw new UnsupportedOperationException("unsupported writer"))
    
  }
}

final class NebulaWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.nebula
  override def write(spark: SparkSession, data: DataFrame, configs: Configs): Unit = {
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
        .withConnectionRetry(2)
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
  override def write(spark: SparkSession, data: DataFrame, configs: Configs): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.write.option("header", true).csv(resultPath)
  }
}

final class TextWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.text
  override def write(spark: SparkSession, data: DataFrame, configs: Configs): Unit = {
    val resultPath = configs.localConfigEntry.resultPath
    data.write.option("header", true).text(resultPath)
  }
}

final class HiveWriter extends AlgoWriter {
  override val tpe: WriterType = WriterType.hive
  override def write(spark: SparkSession, data: DataFrame, configs: Configs): Unit = {
    val config = configs.hiveConfigEntry.hiveWriteConfigEntry
    val saveMode = SaveMode.values().find(_.name.equalsIgnoreCase(config.saveMode)).getOrElse(SaveMode.Append)
    val columnMapping = config.resultColumnMapping

    var _data = data
    columnMapping.map{
      case (from, to) =>
        _data = _data.withColumnRenamed(from, to)
    }

    if(config.autoCreateTable){
      val createTableStatement = generateCreateTableStatement(_data, config.dbTableName)
      println(s"execute create hive table statement:${createTableStatement}")
      spark.sql(createTableStatement)
    }

    println(s"Save to hive:${config.dbTableName}, saveMode:${saveMode}")
    _data.show(3)
    _data.write.mode(saveMode).insertInto(config.dbTableName)
  }

  def generateCreateTableStatement(df: DataFrame, tableName: String): String = {
    val columns = df.schema.fields
    val columnDefinitions = columns.map { field =>
      s"${field.name} ${field.dataType.typeName}"
    }.mkString(",\n  ")
    s"CREATE TABLE IF NOT EXISTS $tableName (\n  $columnDefinitions\n)"
  }

}
