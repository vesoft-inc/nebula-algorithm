/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}

import com.google.common.geometry.{S2CellId, S2LatLng}
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.NebulaCodecImpl
import com.vesoft.nebula.exchange.config.{
  Configs,
  EdgeConfigEntry,
  FileBaseSinkConfigEntry,
  SinkCategory,
  StreamingDataSourceConfigEntry
}
import com.vesoft.nebula.exchange.utils.{HDFSUtils, NebulaUtils}
import com.vesoft.nebula.exchange.{
  Edge,
  Edges,
  ErrorHandler,
  GraphProvider,
  KeyPolicy,
  MetaProvider,
  VidType
}
import org.apache.log4j.Logger
import com.vesoft.nebula.exchange.writer.{NebulaGraphClientWriter, NebulaSSTWriter}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.spark.TaskContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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
    val startTime = System.currentTimeMillis
    iterator.grouped(edgeConfig.batch).foreach { edge =>
      val edges         = Edges(nebulaKeys, edge.toList, edgeConfig.sourcePolicy, edgeConfig.targetPolicy)
      val failStatement = writer.writeEdges(edges)
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
      }
    }
    if (errorBuffer.nonEmpty) {
      ErrorHandler.save(
        errorBuffer,
        s"${config.errorConfig.errorPath}/${edgeConfig.name}.${TaskContext.getPartitionId}")
      errorBuffer.clear()
    }
    LOG.info(
      s"spark partition for edge cost time:" +
        s"${TaskContext.getPartitionId()}-${System.currentTimeMillis() - startTime}")
    writer.close()
    graphProvider.close()
  }

  override def process(): Unit = {

    val address = config.databaseConfig.getMetaAddress
    val space   = config.databaseConfig.space

    val metaProvider    = new MetaProvider(address)
    val fieldTypeMap    = NebulaUtils.getDataSourceFieldType(edgeConfig, space, metaProvider)
    val isVidStringType = metaProvider.getVidType(space) == VidType.STRING
    val partitionNum    = metaProvider.getPartNumber(space)

    if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.SST) {
      val fileBaseConfig = edgeConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
      val namenode       = fileBaseConfig.fsName.orNull
      val edgeName       = edgeConfig.name

      val vidType     = metaProvider.getVidType(space)
      val spaceVidLen = metaProvider.getSpaceVidLen(space)
      val edgeItem    = metaProvider.getEdgeItem(space, edgeName)

      data
        .mapPartitions { iter =>
          iter.map { row =>
            val srcIndex: Int = row.schema.fieldIndex(edgeConfig.sourceField)
            var srcId: String = row.get(srcIndex).toString
            val dstIndex: Int = row.schema.fieldIndex(edgeConfig.targetField)
            var dstId: String = row.get(dstIndex).toString

            if (edgeConfig.sourcePolicy.isDefined) {
              edgeConfig.sourcePolicy.get match {
                case KeyPolicy.HASH =>
                  srcId = MurmurHash2
                    .hash64(srcId.getBytes(), srcId.getBytes().length, 0xc70f6907)
                    .toString
                case KeyPolicy.UUID =>
                  throw new UnsupportedOperationException("do not support uuid yet")
                case _ =>
                  throw new IllegalArgumentException(
                    s"policy ${edgeConfig.sourcePolicy.get} is invalidate")
              }
            }
            if (edgeConfig.targetPolicy.isDefined) {
              edgeConfig.targetPolicy.get match {
                case KeyPolicy.HASH =>
                  dstId = MurmurHash2
                    .hash64(dstId.getBytes(), dstId.getBytes().length, 0xc70f6907)
                    .toString
                case KeyPolicy.UUID =>
                  throw new UnsupportedOperationException("do not support uuid yet")
                case _ =>
                  throw new IllegalArgumentException(
                    s"policy ${edgeConfig.targetPolicy.get} is invalidate")
              }
            }

            val ranking: Long = if (edgeConfig.rankingField.isDefined) {
              val rankIndex = row.schema.fieldIndex(edgeConfig.rankingField.get)
              row.get(rankIndex).toString.toLong
            } else {
              0
            }

            val hostAddrs: ListBuffer[HostAddress] = new ListBuffer[HostAddress]
            for (addr <- address) {
              hostAddrs.append(new HostAddress(addr.getHostText, addr.getPort))
            }

            val partitionId = NebulaUtils.getPartitionId(srcId, partitionNum, vidType)
            val codec       = new NebulaCodecImpl()

            import java.nio.ByteBuffer
            val order = ByteOrder.nativeOrder
            val srcBytes = if (vidType == VidType.INT) {
              ByteBuffer
                .allocate(8)
                .order(ByteOrder.nativeOrder)
                .putLong(srcId.toLong)
                .array
            } else {
              srcId.getBytes()
            }

            val dstBytes = if (vidType == VidType.INT) {
              ByteBuffer
                .allocate(8)
                .order(ByteOrder.nativeOrder)
                .putLong(dstId.toLong)
                .array
            } else {
              dstId.getBytes()
            }
            val positiveEdgeKey = codec.edgeKeyByDefaultVer(spaceVidLen,
                                                            partitionId,
                                                            srcBytes,
                                                            edgeItem.getEdge_type,
                                                            ranking,
                                                            dstBytes)
            val reverseEdgeKey = codec.edgeKeyByDefaultVer(spaceVidLen,
                                                           partitionId,
                                                           dstBytes,
                                                           -edgeItem.getEdge_type,
                                                           ranking,
                                                           srcBytes)

            val values = for {
              property <- fieldKeys if property.trim.length != 0
            } yield
              extraValueForSST(row, property, fieldTypeMap)
                .asInstanceOf[AnyRef]

            val edgeValue = codec.encodeEdge(edgeItem, nebulaKeys.asJava, values.asJava)
            (positiveEdgeKey, reverseEdgeKey, edgeValue)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY, Encoders.BINARY))
        .flatMap(line => {
          List((line._1, line._3), (line._2, line._3))
        })(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
        .toDF("key", "value")
        .sortWithinPartitions("key")
        .foreachPartition { iterator: Iterator[Row] =>
          val taskID                  = TaskContext.get().taskAttemptId()
          var writer: NebulaSSTWriter = null
          var currentPart             = -1
          try {
            iterator.foreach { vertex =>
              val key   = vertex.getAs[Array[Byte]](0)
              val value = vertex.getAs[Array[Byte]](1)
              var part = ByteBuffer
                .wrap(key, 0, 4)
                .order(ByteOrder.nativeOrder)
                .getInt >> 8
              if (part <= 0) {
                part = part + partitionNum
              }

              if (part != currentPart) {
                if (writer != null) {
                  writer.close()
                  val localFile = s"${fileBaseConfig.localPath}/$currentPart-$taskID.sst"
                  HDFSUtils.upload(
                    localFile,
                    s"${fileBaseConfig.remotePath}/${currentPart}/$currentPart-$taskID.sst",
                    namenode)
                  Files.delete(Paths.get(localFile))
                }
                currentPart = part
                val tmp = s"${fileBaseConfig.localPath}/$currentPart-$taskID.sst"
                writer = new NebulaSSTWriter(tmp)
                writer.prepare()
              }
              writer.write(key, value)
            }
          } finally {
            if (writer != null) {
              writer.close()
              val localFile = s"${fileBaseConfig.localPath}/$currentPart-$taskID.sst"
              HDFSUtils.upload(
                localFile,
                s"${fileBaseConfig.remotePath}/${currentPart}/$currentPart-$taskID.sst",
                namenode)
              Files.delete(Paths.get(localFile))
            }
          }
        }
    } else {
      val edgeFrame = data
        .map { row =>
          var sourceField = if (!edgeConfig.isGeo) {
            val sourceIndex = row.schema.fieldIndex(edgeConfig.sourceField)
            assert(sourceIndex >= 0 && row.get(sourceIndex) != null,
                   s"source vertexId must exist and cannot be null, your row data is $row")
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
              assert(NebulaUtils.isNumic(sourceField),
                     s"space vidType is int, but your srcId $sourceField is not numeric.")
            }
          } else {
            assert(!isVidStringType,
                   "only int vidType can use policy, but your vidType is FIXED_STRING.")
          }

          val targetIndex = row.schema.fieldIndex(edgeConfig.targetField)
          assert(targetIndex >= 0 && row.get(targetIndex) != null,
                 s"target vertexId must exist and cannot be null, your row data is $row")
          var targetField = row.get(targetIndex).toString
          if (edgeConfig.targetPolicy.isEmpty) {
            // process string type vid
            if (isVidStringType) {
              targetField = NebulaUtils.escapeUtil(targetField).mkString("\"", "", "\"")
            } else {
              assert(NebulaUtils.isNumic(targetField),
                     s"space vidType is int, but your dstId $targetField is not numeric.")
            }
          } else {
            assert(!isVidStringType,
                   "only int vidType can use policy, but your vidType is FIXED_STRING.")
          }

          val values = for {
            property <- fieldKeys if property.trim.length != 0
          } yield extraValueForClient(row, property, fieldTypeMap)

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
