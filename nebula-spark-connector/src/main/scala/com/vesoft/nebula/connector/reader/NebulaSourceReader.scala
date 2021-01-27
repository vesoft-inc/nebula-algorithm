/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import java.util

import com.vesoft.nebula.connector.{DataTypeEnum, NebulaOptions, NebulaUtils}
import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.meta.ColumnDef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Base class of Nebula Source Reader
  */
abstract class NebulaSourceReader(nebulaOptions: NebulaOptions) extends DataSourceReader {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var datasetSchema: StructType = _

  override def readSchema(): StructType = {
    datasetSchema = getSchema(nebulaOptions)
    LOG.info(s"dataset's schema: $datasetSchema")
    datasetSchema
  }

  protected def getSchema: StructType = getSchema(nebulaOptions)

  /**
    * return the dataset's schema. Schema includes configured cols in returnCols or includes all properties in nebula.
    */
  def getSchema(nebulaOptions: NebulaOptions): StructType = {
    val returnCols                      = nebulaOptions.getReturnCols
    val noColumn                        = nebulaOptions.noColumn
    val fields: ListBuffer[StructField] = new ListBuffer[StructField]
    val metaProvider                    = new MetaProvider(nebulaOptions.getMetaAddress)

    import scala.collection.JavaConverters._
    var schemaCols: Seq[ColumnDef] = Seq()
    val isVertex                   = DataTypeEnum.VERTEX.toString.equalsIgnoreCase(nebulaOptions.dataType)

    // construct vertex or edge default prop
    if (isVertex) {
      fields.append(DataTypes.createStructField("_vertexId", DataTypes.StringType, false))
    } else {
      fields.append(DataTypes.createStructField("_srcId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_dstId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_rank", DataTypes.LongType, false))
    }

    var dataSchema: StructType = null
    // read no column
    if (noColumn) {
      dataSchema = new StructType(fields.toArray)
      return dataSchema
    }
    // get tag schema or edge schema
    val schema = if (isVertex) {
      metaProvider.getTag(nebulaOptions.spaceName, nebulaOptions.label)
    } else {
      metaProvider.getEdge(nebulaOptions.spaceName, nebulaOptions.label)
    }

    schemaCols = schema.columns.asScala

    // read all columns
    if (returnCols.isEmpty) {
      schemaCols.foreach(columnDef => {
        LOG.info(s"prop name ${new String(columnDef.getName)}, type ${columnDef.getType.getType} ")
        fields.append(
          DataTypes.createStructField(new String(columnDef.getName),
                                      NebulaUtils.convertDataType(columnDef.getType),
                                      true))
      })
    } else {
      for (col: String <- returnCols) {
        fields.append(
          DataTypes
            .createStructField(col, NebulaUtils.getColDataType(schemaCols.toList, col), true))
      }
    }
    dataSchema = new StructType(fields.toArray)
    dataSchema
  }
}

/**
  * DataSourceReader for Nebula Vertex
  */
class NebulaDataSourceVertexReader(nebulaOptions: NebulaOptions)
    extends NebulaSourceReader(nebulaOptions) {

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionNum = nebulaOptions.partitionNums.toInt
    val partitions = for (index <- 1 to partitionNum)
      yield {
        new NebulaVertexPartition(index, nebulaOptions, getSchema)
      }
    partitions.map(_.asInstanceOf[InputPartition[InternalRow]]).asJava
  }
}

/**
  * DataSourceReader for Nebula Edge
  */
class NebulaDataSourceEdgeReader(nebulaOptions: NebulaOptions)
    extends NebulaSourceReader(nebulaOptions) {

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionNum = nebulaOptions.partitionNums.toInt
    val partitions = for (index <- 1 to partitionNum)
      yield new NebulaEdgePartition(index, nebulaOptions, getSchema)

    partitions.map(_.asInstanceOf[InputPartition[InternalRow]]).asJava
  }
}
