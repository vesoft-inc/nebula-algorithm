/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{
  DataSourceWriter,
  DataWriter,
  DataWriterFactory,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * creating and initializing the actual Nebula vertex writer at executor side
  */
class NebulaVertexWriterFactory(nebulaOptions: NebulaOptions, vertexIndex: Int, schema: StructType)
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int,
                                taskId: Long,
                                epochId: Long): DataWriter[InternalRow] = {
    new NebulaVertexWriter(nebulaOptions, vertexIndex, schema)
  }
}

/**
  * creating and initializing the actual Nebula edge writer at executor side
  */
class NebulaEdgeWriterFactory(nebulaOptions: NebulaOptions,
                              srcIndex: Int,
                              dstIndex: Int,
                              rankIndex: Option[Int],
                              schema: StructType)
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int,
                                taskId: Long,
                                epochId: Long): DataWriter[InternalRow] = {
    new NebulaEdgeWriter(nebulaOptions, srcIndex, dstIndex, rankIndex, schema)
  }
}

/**
  * nebula vertex writer to create factory
  */
class NebulaDataSourceVertexWriter(nebulaOptions: NebulaOptions,
                                   vertexIndex: Int,
                                   schema: StructType)
    extends DataSourceWriter {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new NebulaVertexWriterFactory(nebulaOptions, vertexIndex, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    LOG.debug(s"${messages.length}")
    for (msg <- messages) {
      val nebulaMsg = msg.asInstanceOf[NebulaCommitMessage]
      LOG.info(s"failed execs:\n ${nebulaMsg.executeStatements.toString()}")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    LOG.error("NebulaDataSourceVertexWriter abort")
  }
}

/**
  * nebula edge writer to create factory
  */
class NebulaDataSourceEdgeWriter(nebulaOptions: NebulaOptions,
                                 srcIndex: Int,
                                 dstIndex: Int,
                                 rankIndex: Option[Int],
                                 schema: StructType)
    extends DataSourceWriter {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new NebulaEdgeWriterFactory(nebulaOptions, srcIndex, dstIndex, rankIndex, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    LOG.debug(s"${messages.length}")
    for (msg <- messages) {
      val nebulaMsg = msg.asInstanceOf[NebulaCommitMessage]
      LOG.info(s"failed execs:\n ${nebulaMsg.executeStatements.toString()}")
    }

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    LOG.error("NebulaDataSourceEdgeWriter abort")
  }
}
