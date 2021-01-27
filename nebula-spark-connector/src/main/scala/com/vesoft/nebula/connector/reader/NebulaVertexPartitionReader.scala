/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.client.storage.scan.{ScanVertexResult, ScanVertexResultIterator}
import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class NebulaVertexPartitionReader(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaPartitionReader(index, nebulaOptions, schema) {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var responseIterator: ScanVertexResultIterator = _

  override def next(): Boolean = {
    if (dataIterator == null && responseIterator == null && !scanPartIterator.hasNext)
      return false

    var continue: Boolean = false
    var break: Boolean    = false
    while ((dataIterator == null || !dataIterator.hasNext) && !break) {
      resultValues.clear()
      continue = false
      if (responseIterator == null || !responseIterator.hasNext) {
        if (scanPartIterator.hasNext) {
          try {
            if (nebulaOptions.noColumn) {
              responseIterator = storageClient.scanVertex(nebulaOptions.spaceName,
                                                          scanPartIterator.next(),
                                                          nebulaOptions.label,
                                                          nebulaOptions.limit,
                                                          0,
                                                          Long.MaxValue,
                                                          true,
                                                          true)
            } else {
              responseIterator = storageClient.scanVertex(nebulaOptions.spaceName,
                                                          scanPartIterator.next(),
                                                          nebulaOptions.label,
                                                          nebulaOptions.getReturnCols.asJava,
                                                          nebulaOptions.limit,
                                                          0,
                                                          Long.MaxValue,
                                                          true,
                                                          true)
            }
          } catch {
            case e: Exception =>
              LOG.error(s"Exception scanning vertex ${nebulaOptions.label}", e)
              storageClient.close()
              throw new Exception(e.getMessage, e)
          }
          // jump to the next loop
          continue = true
        }
        // break while loop
        break = !continue
      } else {
        val next: ScanVertexResult = responseIterator.next
        if (!next.isEmpty) {
          dataIterator = next.getVertexTableRows.iterator().asScala
        }
      }
    }

    if (dataIterator == null) {
      return false
    }
    dataIterator.hasNext
  }

}
