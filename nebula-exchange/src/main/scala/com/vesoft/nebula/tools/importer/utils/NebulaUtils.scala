/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.importer.utils

import com.vesoft.nebula.meta.{PropertyType}
import com.vesoft.nebula.tools.importer.MetaProvider
import com.vesoft.nebula.tools.importer.config.{
  EdgeConfigEntry,
  SchemaConfigEntry,
  TagConfigEntry,
  Type
}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes.{
  BooleanType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType
}
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable

object NebulaUtils {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def getDataSourceFieldType(sourceConfig: SchemaConfigEntry,
                             space: String,
                             metaProvider: MetaProvider): Map[String, Int] = {
    val nebulaFields = sourceConfig.nebulaFields
    val sourceFields = sourceConfig.fields
    val label        = sourceConfig.name

    var nebulaSchemaMap: Map[String, Integer] = null
    val dataType: Type.Value                  = metaProvider.getLabelType(space, label)
    if (dataType == null) {
      throw new IllegalArgumentException(s"label $label does not exist.")
    }
    if (dataType == Type.VERTEX) {
      nebulaSchemaMap = metaProvider.getTagSchema(space, label)
    } else {
      nebulaSchemaMap = metaProvider.getEdgeSchema(space, label)
    }

    val sourceSchemaMap: mutable.Map[String, Int] = mutable.HashMap[String, Int]()
    for (i <- nebulaFields.indices) {
      sourceSchemaMap.put(sourceFields.get(i), nebulaSchemaMap(nebulaFields.get(i)))
    }
    // todo String vid and Int vid
    if (dataType == Type.VERTEX) {
      sourceSchemaMap.put(sourceConfig.asInstanceOf[TagConfigEntry].vertexField,
                          PropertyType.STRING)
    } else {
      sourceSchemaMap.put(sourceConfig.asInstanceOf[EdgeConfigEntry].sourceField,
                          PropertyType.STRING)
      sourceSchemaMap.put(sourceConfig.asInstanceOf[EdgeConfigEntry].targetField,
                          PropertyType.STRING)
    }
    sourceSchemaMap.toMap
  }

  def getDataType(clazz: Class[_]): DataType = {
    if (classOf[java.lang.Boolean] == clazz) return BooleanType
    else if (classOf[java.lang.Long] == clazz || classOf[java.lang.Integer] == clazz)
      return LongType
    else if (classOf[java.lang.Double] == clazz || classOf[java.lang.Float] == clazz)
      return DoubleType
    StringType
  }

  def getDataFrameValue(value: String, dataType: DataType): Any = {
    dataType match {
      case LongType    => value.toLong
      case IntegerType => value.toInt
      case BooleanType => value.toBoolean
      case DoubleType  => value.toDouble
      case FloatType   => value.toFloat
      case _           => value
    }
  }

  def isNumic(str: String): Boolean = {
    for (char <- str.toCharArray) {
      if (!Character.isDigit(char)) return false
    }
    true
  }
}
