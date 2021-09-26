/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}

import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.NebulaCodecImpl
import com.vesoft.nebula.exchange.{
  ErrorHandler,
  GraphProvider,
  KeyPolicy,
  MetaProvider,
  Vertex,
  Vertices,
  VidType
}
import com.vesoft.nebula.exchange.config.{
  Configs,
  FileBaseSinkConfigEntry,
  SinkCategory,
  StreamingDataSourceConfigEntry,
  TagConfigEntry
}
import com.vesoft.nebula.exchange.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import com.vesoft.nebula.exchange.utils.{HDFSUtils, NebulaUtils}
import com.vesoft.nebula.exchange.writer.{NebulaGraphClientWriter, NebulaSSTWriter}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
  * @param data
  * @param tagConfig
  * @param fieldKeys
  * @param nebulaKeys
  * @param config
  * @param batchSuccess
  * @param batchFailure
  */
class VerticesProcessor(data: DataFrame,
                        tagConfig: TagConfigEntry,
                        fieldKeys: List[String],
                        nebulaKeys: List[String],
                        config: Configs,
                        batchSuccess: LongAccumulator,
                        batchFailure: LongAccumulator)
    extends Processor {

  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  private def processEachPartition(iterator: Iterator[Vertex]): Unit = {
    val graphProvider =
      new GraphProvider(config.databaseConfig.getGraphAddress, config.connectionConfig.timeout)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
                                             config.userConfig,
                                             config.rateConfig,
                                             tagConfig,
                                             graphProvider)

    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write tags
    val startTime = System.currentTimeMillis
    iterator.grouped(tagConfig.batch).foreach { vertex =>
      val vertices      = Vertices(nebulaKeys, vertex.toList, tagConfig.vertexPolicy)
      val failStatement = writer.writeVertices(vertices)
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
        s"${config.errorConfig.errorPath}/${tagConfig.name}.${TaskContext.getPartitionId()}")
      errorBuffer.clear()
    }
    LOG.info(s"tag ${tagConfig.name} import in spark partition ${TaskContext
      .getPartitionId()} cost ${System.currentTimeMillis() - startTime} ms")
    writer.close()
    graphProvider.close()
  }

  override def process(): Unit = {

    val address = config.databaseConfig.getMetaAddress
    val space   = config.databaseConfig.space

    val timeout         = config.connectionConfig.timeout
    val retry           = config.connectionConfig.retry
    val metaProvider    = new MetaProvider(address, timeout, retry)
    val fieldTypeMap    = NebulaUtils.getDataSourceFieldType(tagConfig, space, metaProvider)
    val isVidStringType = metaProvider.getVidType(space) == VidType.STRING
    val partitionNum    = metaProvider.getPartNumber(space)

    if (tagConfig.dataSinkConfigEntry.category == SinkCategory.SST) {
      val fileBaseConfig = tagConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
      val namenode       = fileBaseConfig.fsName.orNull
      val tagName        = tagConfig.name
      val vidType        = metaProvider.getVidType(space)

      val spaceVidLen = metaProvider.getSpaceVidLen(space)
      val tagItem     = metaProvider.getTagItem(space, tagName)

      data
        .dropDuplicates(tagConfig.vertexField)
        .mapPartitions { iter =>
          iter.map { row =>
            val index: Int = row.schema.fieldIndex(tagConfig.vertexField)
            assert(index >= 0 && !row.isNullAt(index),
                   s"vertexId must exist and cannot be null, your row data is $row")
            var vertexId: String = row.get(index).toString
            if (vertexId.equals(DEFAULT_EMPTY_VALUE)) {
              vertexId = ""
            }
            if (tagConfig.vertexPolicy.isDefined) {
              tagConfig.vertexPolicy.get match {
                case KeyPolicy.HASH =>
                  vertexId = MurmurHash2
                    .hash64(vertexId.getBytes(), vertexId.getBytes().length, 0xc70f6907)
                    .toString
                case KeyPolicy.UUID =>
                  throw new UnsupportedOperationException("do not support uuid yet")
                case _ =>
                  throw new IllegalArgumentException(
                    s"policy ${tagConfig.vertexPolicy.get} is invalidate")
              }
            }

            val hostAddrs: ListBuffer[HostAddress] = new ListBuffer[HostAddress]
            for (addr <- address) {
              hostAddrs.append(new HostAddress(addr.getHostText, addr.getPort))
            }

            val partitionId = NebulaUtils.getPartitionId(vertexId, partitionNum, vidType)

            val codec = new NebulaCodecImpl()

            import java.nio.ByteBuffer
            val vidBytes = if (vidType == VidType.INT) {
              ByteBuffer
                .allocate(8)
                .order(ByteOrder.nativeOrder)
                .putLong(vertexId.toLong)
                .array
            } else {
              vertexId.getBytes()
            }

            val vertexKey = codec.vertexKey(spaceVidLen, partitionId, vidBytes, tagItem.getTag_id)
            val values = for {
              property <- fieldKeys if property.trim.length != 0
            } yield
              extraValueForSST(row, property, fieldTypeMap)
                .asInstanceOf[AnyRef]
            val vertexValue = codec.encodeTag(tagItem, nebulaKeys.asJava, values.asJava)
            (vertexKey, vertexValue)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
        .toDF("key", "value")
        .sortWithinPartitions("key")
        .foreachPartition { iterator: Iterator[Row] =>
          val taskID                  = TaskContext.get().taskAttemptId()
          var writer: NebulaSSTWriter = null
          var currentPart             = -1
          val localPath               = fileBaseConfig.localPath
          val remotePath              = fileBaseConfig.remotePath
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
                  val localFile = s"$localPath/$currentPart-$taskID.sst"
                  HDFSUtils.upload(localFile,
                                   s"$remotePath/${currentPart}/$currentPart-$taskID.sst",
                                   namenode)
                  Files.delete(Paths.get(localFile))
                }
                currentPart = part
                val tmp = s"$localPath/$currentPart-$taskID.sst"
                writer = new NebulaSSTWriter(tmp)
                writer.prepare()
              }
              writer.write(key, value)
            }
          } catch {
            case e: Throwable => {
              LOG.error(e)
              batchFailure.add(1)
            }
          } finally {
            if (writer != null) {
              writer.close()
              val localFile = s"$localPath/$currentPart-$taskID.sst"
              HDFSUtils.upload(localFile,
                               s"$remotePath/${currentPart}/$currentPart-$taskID.sst",
                               namenode)
              Files.delete(Paths.get(localFile))
            }
          }
        }
    } else {
      val vertices = data
        .map { row =>
          val vertexID = {
            val index = row.schema.fieldIndex(tagConfig.vertexField)
            assert(index >= 0 && !row.isNullAt(index),
                   s"vertexId must exist and cannot be null, your row data is $row")
            var value = row.get(index).toString
            if (value.equals(DEFAULT_EMPTY_VALUE)) { value = "" }
            if (tagConfig.vertexPolicy.isEmpty) {
              // process string type vid
              if (isVidStringType) {
                NebulaUtils.escapeUtil(value).mkString("\"", "", "\"")
              } else {
                // process int type vid
                assert(NebulaUtils.isNumic(value),
                       s"space vidType is int, but your vertex id $value is not numeric.")
                value
              }
            } else {
              assert(!isVidStringType,
                     "only int vidType can use policy, but your vidType is FIXED_STRING.")
              value
            }
          }

          val values = for {
            property <- fieldKeys if property.trim.length != 0
          } yield extraValueForClient(row, property, fieldTypeMap)
          Vertex(vertexID, values)
        }(Encoders.kryo[Vertex])

      // streaming write
      if (data.isStreaming) {
        val streamingDataSourceConfig =
          tagConfig.dataSourceConfigEntry.asInstanceOf[StreamingDataSourceConfigEntry]
        vertices.writeStream
          .foreachBatch((vertexSet, batchId) => {
            LOG.info(s"${tagConfig.name} tag start batch ${batchId}.")
            vertexSet.foreachPartition(processEachPartition _)
          })
          .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
          .start()
          .awaitTermination()
      } else
        vertices.foreachPartition(processEachPartition _)
    }
  }
}
