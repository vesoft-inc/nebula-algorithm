/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.config

import com.vesoft.nebula.exchange.KeyPolicy

/**
  * SchemaConfigEntry is tag/edge super class use to save some basic parameter for importer.
  */
sealed trait SchemaConfigEntry {

  /** nebula tag or edge name */
  def name: String

  /** see{@link DataSourceConfigEntry}*/
  def dataSourceConfigEntry: DataSourceConfigEntry

  /** see{@link DataSinkConfigEntry}*/
  def dataSinkConfigEntry: DataSinkConfigEntry

  /** data source fields which are going to be import to nebula as properties */
  def fields: List[String]

  /** nebula properties which are going to fill value with data source value*/
  def nebulaFields: List[String]

  /** vertex or edge amount of one batch import */
  def batch: Int

  /** spark partition */
  def partition: Int

  /** check point path */
  def checkPointPath: Option[String]
}

/**
  *
  * @param name
  * @param dataSourceConfigEntry
  * @param dataSinkConfigEntry
  * @param fields
  * @param nebulaFields
  * @param vertexField
  * @param vertexPolicy
  * @param batch
  * @param partition
  * @param checkPointPath
  */
case class TagConfigEntry(override val name: String,
                          override val dataSourceConfigEntry: DataSourceConfigEntry,
                          override val dataSinkConfigEntry: DataSinkConfigEntry,
                          override val fields: List[String],
                          override val nebulaFields: List[String],
                          vertexField: String,
                          vertexPolicy: Option[KeyPolicy.Value],
                          override val batch: Int,
                          override val partition: Int,
                          override val checkPointPath: Option[String])
    extends SchemaConfigEntry {
  require(name.trim.nonEmpty && vertexField.trim.nonEmpty && batch > 0)

  override def toString: String = {
    s"Tag name: $name, " +
      s"source: $dataSourceConfigEntry, " +
      s"sink: $dataSinkConfigEntry, " +
      s"vertex field: $vertexField, " +
      s"vertex policy: $vertexPolicy, " +
      s"batch: $batch, " +
      s"partition: $partition."
  }
}

/**
  *
  * @param name
  * @param dataSourceConfigEntry
  * @param dataSinkConfigEntry
  * @param fields
  *  @param nebulaFields
  * @param sourceField
  * @param sourcePolicy
  * @param rankingField
  * @param targetField
  * @param targetPolicy
  * @param isGeo
  * @param latitude
  * @param longitude
  * @param batch
  * @param partition
  * @param checkPointPath
  */
case class EdgeConfigEntry(override val name: String,
                           override val dataSourceConfigEntry: DataSourceConfigEntry,
                           override val dataSinkConfigEntry: DataSinkConfigEntry,
                           override val fields: List[String],
                           override val nebulaFields: List[String],
                           sourceField: String,
                           sourcePolicy: Option[KeyPolicy.Value],
                           rankingField: Option[String],
                           targetField: String,
                           targetPolicy: Option[KeyPolicy.Value],
                           isGeo: Boolean,
                           latitude: Option[String],
                           longitude: Option[String],
                           override val batch: Int,
                           override val partition: Int,
                           override val checkPointPath: Option[String])
    extends SchemaConfigEntry {
  require(
    name.trim.nonEmpty && sourceField.trim.nonEmpty &&
      targetField.trim.nonEmpty && batch > 0)

  override def toString: String = {
    if (isGeo) {
      s"Edge name: $name, " +
        s"source: $dataSourceConfigEntry, " +
        s"sink: $dataSinkConfigEntry, " +
        s"latitude: $latitude, " +
        s"longitude: $longitude, " +
        s"source field: $sourceField, " +
        s"source policy: $sourcePolicy, " +
        s"ranking: $rankingField, " +
        s"target field: $targetField, " +
        s"target policy: $targetPolicy, " +
        s"batch: $batch, " +
        s"partition: $partition."
    } else {
      s"Edge name: $name, " +
        s"source: $dataSourceConfigEntry, " +
        s"sink: $dataSinkConfigEntry, " +
        s"source field: $sourceField, " +
        s"source policy: $sourcePolicy, " +
        s"ranking: $rankingField, " +
        s"target field: $targetField, " +
        s"target policy: $targetPolicy, " +
        s"batch: $batch, " +
        s"partition: $partition."
    }
  }
}
