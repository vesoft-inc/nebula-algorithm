/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.importer.processor

import com.google.common.geometry.{S2CellId, S2LatLng}
import com.vesoft.nebula.tools.importer.config.{
  Configs,
  EdgeConfigEntry,
  SinkCategory,
  StreamingDataSourceConfigEntry
}
import com.vesoft.nebula.tools.importer.utils.NebulaUtils
import com.vesoft.nebula.tools.importer.{
  Edge,
  Edges,
  ErrorHandler,
  GraphProvider,
  MetaProvider,
  VidType
}
import org.apache.log4j.Logger
import com.vesoft.nebula.tools.importer.writer.NebulaGraphClientWriter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class EdgeProcessor(data: DataFrame,
                    edgeConfig: EdgeConfigEntry,
                    fieldKeys: List[String],
                    nebulaKeys: List[String],
                    config: Configs,
                    batchSuccess: LongAccumulator,
                    batchFailure: LongAccumulator)
    extends Processor {

  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  private[this] val DEFAULT_MIN_CELL_LEVEL = 10
  private[this] val DEFAULT_MAX_CELL_LEVEL = 18

  private def processEachPartition(iterator: Iterator[Edge]): Unit = {
    val graphProvider = new GraphProvider(config.databaseConfig.getGraphAddress)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
                                             config.userConfig,
                                             config.connectionConfig,
                                             config.executionConfig.retry,
                                             config.rateConfig,
                                             edgeConfig,
                                             graphProvider)
    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write tags
    iterator.grouped(edgeConfig.batch).foreach { edge =>
      val edges         = Edges(nebulaKeys, edge.toList, edgeConfig.sourcePolicy, edgeConfig.targetPolicy)
      val failStatement = writer.writeEdges(edges)
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
      }

      if (errorBuffer.nonEmpty) {
        ErrorHandler.save(errorBuffer, s"${config.errorConfig.errorPath}/${edgeConfig.name}")
        errorBuffer.clear()
      }
    }
    writer.close()
  }

  override def process(): Unit = {

    val address = config.databaseConfig.getMetaAddress
    val space   = config.databaseConfig.space

    val metaProvider    = new MetaProvider(address)
    val fieldTypeMap    = NebulaUtils.getDataSourceFieldType(edgeConfig, space, metaProvider)
    val isVidStringType = metaProvider.getVidType(space) == VidType.STRING

    if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.SST) {} else {
      val edgeFrame = data
        .map { row =>
          var sourceField = if (!edgeConfig.isGeo) {
            val sourceIndex = row.schema.fieldIndex(edgeConfig.sourceField)
            row.get(sourceIndex).toString
          } else {
            val lat = row.getDouble(row.schema.fieldIndex(edgeConfig.latitude.get))
            val lng = row.getDouble(row.schema.fieldIndex(edgeConfig.longitude.get))
            indexCells(lat, lng).mkString(",")
          }

          if (edgeConfig.sourcePolicy.isEmpty) {
            // process string type vid
            if (isVidStringType) {
              sourceField = NebulaUtils.escapeUtil(sourceField).mkString("\"", "", "\"")
            } else {
              assert(NebulaUtils.isNumic(sourceField))
            }
          }

          val targetIndex = row.schema.fieldIndex(edgeConfig.targetField)
          var targetField = row.get(targetIndex).toString
          if (edgeConfig.targetPolicy.isEmpty) {
            // process string type vid
            if (isVidStringType) {
              targetField = NebulaUtils.escapeUtil(targetField).mkString("\"", "", "\"")
            } else {
              assert(NebulaUtils.isNumic(targetField))
            }
          }

          val values = for {
            property <- fieldKeys if property.trim.length != 0
          } yield extraValue(row, property, fieldTypeMap)

          if (edgeConfig.rankingField.isDefined) {
            val index   = row.schema.fieldIndex(edgeConfig.rankingField.get)
            val ranking = row.get(index).toString
            assert(NebulaUtils.isNumic(ranking), s"Not support non-Numeric type for ranking field")

            Edge(sourceField, targetField, Some(ranking.toLong), values)
          } else {
            Edge(sourceField, targetField, None, values)
          }
        }(Encoders.kryo[Edge])

      // streaming write
      if (data.isStreaming) {
        val streamingDataSourceConfig =
          edgeConfig.dataSourceConfigEntry.asInstanceOf[StreamingDataSourceConfigEntry]
        edgeFrame.writeStream
          .foreachBatch((edges, batchId) => {
            LOG.info(s"${edgeConfig.name} edge start batch ${batchId}.")
            edges.foreachPartition(processEachPartition _)
          })
          .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
          .start()
          .awaitTermination()
      } else
        edgeFrame.foreachPartition(processEachPartition _)
    }
  }

  private[this] def indexCells(lat: Double, lng: Double): IndexedSeq[Long] = {
    val coordinate = S2LatLng.fromDegrees(lat, lng)
    val s2CellId   = S2CellId.fromLatLng(coordinate)
    for (index <- DEFAULT_MIN_CELL_LEVEL to DEFAULT_MAX_CELL_LEVEL)
      yield s2CellId.parent(index).id()
  }
}
