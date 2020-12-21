/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import com.vesoft.nebula.meta.PropertyType
import com.vesoft.nebula.exchange.utils.{HDFSUtils, NebulaUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
  * processor is a converter.
  * It is responsible for converting the dataframe row data into Nebula Graph's vertex or edge,
  * and submit data to writer.
  */
trait Processor extends Serializable {

  /**
    * process dataframe to vertices or edges
    */
  def process(): Unit

  /**
    * handle special types of attributes
    *
    * String type： add "" for attribute value， if value contains escape symbol，then keep it.
    *
    * Date type: add date() function for attribute value.
    * eg: convert attribute value 2020-01-01 to date("2020-01-01")
    *
    * Time type: add time() function for attribute value.
    * eg: convert attribute value 12:12:12:1111 to time("12:12:12:1111")
    *
    * DataTime type: add datetime() function for attribute value.
    * eg: convert attribute value 2020-01-01T22:30:40 to datetime("2020-01-01T22:30:40")
    */
  def extraValue(row: Row,
                 field: String,
                 fieldTypeMap: Map[String, Int],
                 toBytes: Boolean = false): Any = {
    val index = row.schema.fieldIndex(field)

    if (row.isNullAt(index)) return null

    fieldTypeMap(field) match {
      case PropertyType.STRING => {
        val result = NebulaUtils.escapeUtil(row.get(index).toString).mkString("\"", "", "\"")
        if (toBytes) result.getBytes else result
      }
      case PropertyType.DATE     => "date(\"" + row.get(index) + "\")"
      case PropertyType.DATETIME => "datatime(\"" + row.get(index) + "\")"
      case PropertyType.TIME     => "time(\"" + row.get(index) + "\")"
      case _                     => row.get(index)
    }
  }

  def fetchOffset(path: String): Long = {
    HDFSUtils.getContent(path).toLong
  }

  def getLong(row: Row, field: String): Long = {
    val index = row.schema.fieldIndex(field)
    row.schema.fields(index).dataType match {
      case LongType    => row.getLong(index)
      case IntegerType => row.getInt(index).toLong
      case StringType  => row.getString(index).toLong
    }
  }
}
