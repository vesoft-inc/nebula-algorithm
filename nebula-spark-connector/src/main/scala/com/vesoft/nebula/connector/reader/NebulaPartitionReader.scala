/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.client.graph.data.{HostAddress, ValueWrapper}
import com.vesoft.nebula.client.storage.StorageClient
import com.vesoft.nebula.client.storage.data.{BaseTableRow, VertexTableRow}
import com.vesoft.nebula.connector.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.connector.exception.GraphConnectException
import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils, PartitionUtils}
import com.vesoft.nebula.connector.nebula.MetaProvider
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Read nebula data for each spark partition
  */
abstract class NebulaPartitionReader extends InputPartitionReader[InternalRow] {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var metaProvider: MetaProvider = _
  private var schema: StructType         = _

  protected var dataIterator: Iterator[BaseTableRow]           = _
  protected var scanPartIterator: Iterator[Integer]            = _
  protected var resultValues: mutable.ListBuffer[List[Object]] = mutable.ListBuffer[List[Object]]()
  protected var storageClient: StorageClient                   = _

  /**
    * @param index identifier for spark partition
    * @param nebulaOptions nebula Options
    * @param schema of data need to read
    */
  def this(index: Int, nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    this.schema = schema

    metaProvider = new MetaProvider(nebulaOptions.getMetaAddress)
    val address: ListBuffer[HostAddress] = new ListBuffer[HostAddress]

    for (addr <- nebulaOptions.getMetaAddress) {
      address.append(new HostAddress(addr._1, addr._2))
    }

    this.storageClient = new StorageClient(address.asJava)
    if (!storageClient.connect()) {
      throw new GraphConnectException("storage connect failed.")
    }
    // allocate scanPart to this partition
    val totalPart = metaProvider.getPartitionNumber(nebulaOptions.spaceName)

    val scanParts = PartitionUtils.getScanParts(index, totalPart, nebulaOptions.partitionNums.toInt)
    LOG.info(s"partition index: ${index}, scanParts: ${scanParts.toString}")
    scanPartIterator = scanParts.iterator
  }

  override def get(): InternalRow = {
    val resultSet: Array[ValueWrapper] =
      dataIterator.next().getValues.toArray.map(v => v.asInstanceOf[ValueWrapper])
    val getters: Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                        = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    for (i <- getters.indices) {
      val value: ValueWrapper = resultSet(i)
      var resolved            = false
      if (value.isNull) {
        mutableRow.setNullAt(i)
        resolved = true
      }
      if (value.isString) {
        getters(i).apply(value.asString(), mutableRow, i)
        resolved = true
      }
      if (value.isDate) {
        getters(i).apply(value.asDate(), mutableRow, i)
        resolved = true
      }
      if (value.isTime) {
        getters(i).apply(value.asTime(), mutableRow, i)
        resolved = true
      }
      if (value.isDateTime) {
        getters(i).apply(value.asDateTime(), mutableRow, i)
        resolved = true
      }
      if (value.isLong) {
        getters(i).apply(value.asLong(), mutableRow, i)
      }
      if (value.isBoolean) {
        getters(i).apply(value.asBoolean(), mutableRow, i)
      }
      if (value.isDouble) {
        getters(i).apply(value.asDouble(), mutableRow, i)
      }
    }
    mutableRow
  }

  override def close(): Unit = {
    metaProvider.close()
    storageClient.close()
  }
}
