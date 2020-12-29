/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import java.util

import com.vesoft.nebula.connector.NebulaOptions
import com.vesoft.nebula.connector.reader.tmp.ScanEdgeResult
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

class NebulaEdgePartitionReader(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaPartitionReader(index, nebulaOptions, schema) {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var responseIterator: util.Iterator[ScanEdgeResult] = _

  override def next(): Boolean = {
//    if (dataIterator == null && responseIterator == null && !scanPartIterator.hasNext) return false
//
//    var continue: Boolean = false
//    var break: Boolean    = false
//    while ((dataIterator == null || !dataIterator.hasNext) && !break) {
//      resultValues.clear()
//      continue = false
//      if (responseIterator == null || !responseIterator.hasNext) {
//        if (scanPartIterator.hasNext) {
//          try {
//            if (nebulaOptions.noColumn) {
//              responseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
//                                                        scanPartIterator.next(),
//                                                        nebulaOptions.label,
//                                                        nebulaOptions.limit)
//            } else {
//              responseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
//                                                        scanPartIterator.next(),
//                                                        nebulaOptions.label,
//                                                        nebulaOptions.getReturnCols,
//                                                        nebulaOptions.limit)
//            }
//          } catch {
//            case e: Exception =>
//              LOG.error(s"Exception scanning vertex ${nebulaOptions.label}", e)
//              throw new Exception(e.getMessage, e)
//          }
//          // jump to the next loop
//          continue = true
//        }
//        // break while loop
//        break = !continue
//      } else {
//        val next: ScanEdgeResult = responseIterator.next
//        // todo dataIterator = next.getVertexTableView
//      }
//    }
//
//    if (dataIterator == null) {
//      return false
//    }
//    dataIterator.hasNext
    false
  }

}
