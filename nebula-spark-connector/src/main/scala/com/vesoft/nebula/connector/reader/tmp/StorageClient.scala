/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader.tmp

class StorageClient {
  def scanVertex(): ScanVertexResultIterator = {
    new ScanVertexResultIterator
  }

  def scanEdge(): ScanEdgeResultIterator = {
    new ScanEdgeResultIterator
  }
}
