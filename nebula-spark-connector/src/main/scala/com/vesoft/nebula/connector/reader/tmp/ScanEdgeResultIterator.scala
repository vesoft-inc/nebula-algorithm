/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader.tmp

class ScanEdgeResultIterator {

  def next(): ScanEdgeResult = {
    new ScanEdgeResult
  }

  def hasNext(): Boolean = {
    true
  }
}

class ScanEdgeResult {
  val edgeTableViews: List[EdgeTableView] = null

  val propNames: List[String] = null

}

class EdgeTableView {}
