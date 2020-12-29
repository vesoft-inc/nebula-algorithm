/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class NebulaVertexPartition(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new NebulaVertexPartitionReader(index, nebulaOptions, schema)
}

class NebulaEdgePartition(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new NebulaEdgePartitionReader(index, nebulaOptions, schema)
}
