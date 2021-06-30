/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import com.vesoft.nebula.exchange.{ErrorHandler, GraphProvider}
import com.vesoft.nebula.exchange.config.Configs
import com.vesoft.nebula.exchange.writer.NebulaGraphClientWriter
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class ReloadProcessor(data: DataFrame,
                      config: Configs,
                      batchSuccess: LongAccumulator,
                      batchFailure: LongAccumulator)
    extends Processor {
  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  override def process(): Unit = {
    data.foreachPartition(processEachPartition(_))
    ErrorHandler.clear(config.errorConfig.errorPath)
  }

  private def processEachPartition(iterator: Iterator[Row]): Unit = {
    val graphProvider =
      new GraphProvider(config.databaseConfig.getGraphAddress, config.connectionConfig.timeout)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
                                             config.userConfig,
                                             config.rateConfig,
                                             null,
                                             graphProvider)

    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write
    val startTime = System.currentTimeMillis
    iterator.foreach { row =>
      val failStatement = writer.writeNgql(row.getString(0))
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
      }
    }
    if (errorBuffer.nonEmpty) {
      ErrorHandler.save(errorBuffer,
                        s"${config.errorConfig.errorPath}/reload.${TaskContext.getPartitionId()}")
      errorBuffer.clear()
    }
    LOG.info(s"data reload in partition ${TaskContext
      .getPartitionId()} cost ${System.currentTimeMillis() - startTime}ms")
    writer.close()
    graphProvider.close()
  }
}
