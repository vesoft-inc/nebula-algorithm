/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import java.util.Map.Entry
import java.util.Optional

import com.vesoft.nebula.connector.exception.IllegalOptionException
import com.vesoft.nebula.connector.reader.{NebulaDataSourceEdgeReader, NebulaDataSourceVertexReader}
import com.vesoft.nebula.connector.writer.{NebulaDataSourceEdgeWriter, NebulaDataSourceVertexWriter}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.iterableAsScalaIterable

class NebulaDataSource
    extends DataSourceV2
    with ReadSupport
    with WriteSupport
    with DataSourceRegister {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * The string that represents the format that nebula data source provider uses.
    */
  override def shortName(): String = "nebula"

  /**
    * Creates a {@link DataSourceReader} to scan the data from Nebula Graph.
    */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val nebulaOptions = getNebulaOptions(options, OperaType.READ)
    val dataType      = nebulaOptions.dataType

    LOG.info("create reader")
    LOG.info(s"options ${options.asMap()}")

    if (DataTypeEnum.VERTEX == DataTypeEnum.withName(dataType)) {
      new NebulaDataSourceVertexReader(nebulaOptions)
    } else {
      new NebulaDataSourceEdgeReader(nebulaOptions)
    }
  }

  /**
    * Creates an optional {@link DataSourceWriter} to save the data to Nebula Graph.
    */
  override def createWriter(writeUUID: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {

    val nebulaOptions = getNebulaOptions(options, OperaType.WRITE)
    val dataType      = nebulaOptions.dataType
    if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
      LOG.warn(s"Currently do not support mode")
    }

    LOG.info("create writer")
    LOG.info(s"options ${options.asMap()}")

    if (DataTypeEnum.VERTEX == DataTypeEnum.withName(dataType)) {
      val vertexFiled = nebulaOptions.vertexField
      val vertexIndex: Int = {
        var index: Int = -1
        for (i <- schema.fields.indices) {
          if (schema.fields(i).name.equals(vertexFiled)) {
            index = i
          }
        }
        if (index < 0) {
          throw new IllegalOptionException(
            s" vertex field ${vertexFiled} does not exist in dataframe")
        }
        index
      }
      Optional.of(new NebulaDataSourceVertexWriter(nebulaOptions, vertexIndex, schema))
    } else {
      val srcVertexFiled = nebulaOptions.srcVertexField
      val dstVertexField = nebulaOptions.dstVertexField
      val rankExist      = !nebulaOptions.rankField.isEmpty
      val edgeFieldsIndex = {
        var srcIndex: Int  = -1
        var dstIndex: Int  = -1
        var rankIndex: Int = -1
        for (i <- schema.fields.indices) {
          if (schema.fields(i).name.equals(srcVertexFiled)) {
            srcIndex = i
          }
          if (schema.fields(i).name.equals(dstVertexField)) {
            dstIndex = i
          }
          if (rankExist) {
            if (schema.fields(i).name.equals(nebulaOptions.rankField)) {
              rankIndex = i
            }
          }
        }
        // check src filed and dst field
        if (srcIndex < 0 || dstIndex < 0) {
          throw new IllegalOptionException(
            s" srcVertex field ${srcVertexFiled} or dstVertex field ${dstVertexField} do not exist in dataframe")
        }
        // check rank field
        if (rankExist && rankIndex < 0) {
          throw new IllegalOptionException(s"rank field does not exist in dataframe")
        }

        if (!rankExist) {
          (srcIndex, dstIndex, Option.empty)
        } else {
          (srcIndex, dstIndex, Option(rankIndex))
        }

      }
      Optional.of(
        new NebulaDataSourceEdgeWriter(nebulaOptions,
                                       edgeFieldsIndex._1,
                                       edgeFieldsIndex._2,
                                       edgeFieldsIndex._3,
                                       schema))
    }
  }

  /**
    * construct nebula options with DataSourceOptions
    */
  def getNebulaOptions(options: DataSourceOptions, operateType: OperaType.Value): NebulaOptions = {
    var parameters: Map[String, String] = Map()
    for (entry: Entry[String, String] <- options.asMap().entrySet) {
      parameters += (entry.getKey -> entry.getValue)
    }
    val nebulaOptions = new NebulaOptions(CaseInsensitiveMap(parameters))(operateType)
    nebulaOptions
  }
}
