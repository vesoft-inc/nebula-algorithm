/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{NebulaOptions, PartitionUtils}
import com.vesoft.nebula.connector.nebula.MetaProvider
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

/**
  * Read nebula data for each spark partition
  */
abstract class NebulaPartitionReader extends InputPartitionReader[InternalRow] {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

//  protected var dataIterator: Iterator[List[Property]] = _
  protected var scanPartIterator: Iterator[Integer] = _
//  protected var resultValues: mutable.ListBuffer[List[Property]] =
//    mutable.ListBuffer[List[Property]]()
//
//  todo 对接2.0
//  protected var storageClient: StorageClient = _
  private var schema: StructType = _

  private var metaProvider: MetaProvider = _

  /**
    * @param index identifier for spark partition
    * @param nebulaOptions nebula Options
    * @param schema of data need to read
    */
  def this(index: Int, nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    this.schema = schema

    metaProvider = new MetaProvider(nebulaOptions.getMetaAddress)
    // todo this.storageClient = new StorageClient
    // allocate scanPart to this partition
    val totalPart = metaProvider.getPartitionNumber(nebulaOptions.spaceName)

    val scanParts = PartitionUtils.getScanParts(index, totalPart, nebulaOptions.partitionNums.toInt)
    LOG.info(s"partition index: ${index}, scanParts: ${scanParts.toString}")
    scanPartIterator = scanParts.iterator
  }

  override def get(): InternalRow = {
//    val getters: Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
//    val mutableRow                        = new SpecificInternalRow(schema.fields.map(x => x.dataType))
//
//    val resultSet: Array[Property] = dataIterator.next().toArray
//    for (i <- getters.indices) {
//      getters(i).apply(resultSet(i), mutableRow, i)
//      if (resultSet(i) == null) mutableRow.setNullAt(i)
//    }
//    mutableRow
    null
  }

  override def close(): Unit = {
    metaProvider.close()
  }
}
