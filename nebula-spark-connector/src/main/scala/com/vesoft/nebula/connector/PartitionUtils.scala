/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import scala.collection.mutable.ListBuffer

object PartitionUtils {
  def getScanParts(index: Int, nebulaTotalPart: Int, sparkPartitionNum: Int): List[Integer] = {
    val scanParts   = new ListBuffer[Integer]
    var currentPart = index
    while (currentPart <= nebulaTotalPart) {
      scanParts.append(currentPart)
      currentPart += sparkPartitionNum
    }
    scanParts.toList
  }

}
