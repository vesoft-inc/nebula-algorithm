/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.importer.processor

import com.vesoft.nebula.tools.importer.{ErrorHandler, GraphProvider}
import com.vesoft.nebula.tools.importer.config.Configs
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class ReloadProcessor(data: DataFrame,
                      config: Configs,
                      batchSuccess: LongAccumulator,
                      batchFailure: LongAccumulator)
    extends Processor {

  override def process(): Unit = {
    data.foreachPartition(processEachPartition(_))
  }

  private def processEachPartition(iterator: Iterator[Row]): Unit = {
    val graphProvider = new GraphProvider(config.databaseConfig.getGraphAddress)
    val session       = graphProvider.getGraphClient(config.userConfig)
    if (session == null) {
      throw new IllegalArgumentException("connect to graph failed.")
    }

    val errorBuffer = ArrayBuffer[String]()

    iterator.foreach(row => {
      val exec   = row.getString(0)
      val result = session.execute(exec)
      if (result == null || !result.isSucceeded) {
        errorBuffer.append(exec)
        batchFailure.add(1)
      } else {
        batchSuccess.add(1)
      }
      if (errorBuffer.nonEmpty) {
        ErrorHandler.save(errorBuffer, s"${config.errorConfig.errorPath}/reload")
        errorBuffer.clear()
      }
    })
  }
}
