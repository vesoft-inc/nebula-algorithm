/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.utils

import org.neo4j.driver.Value

object Neo4jUtils {

  def convertNeo4jData(value: Value): String = {
    value.`type`().name() match {
      case "NULL" => {
        null
      }
      case "STRING" => {
        value.asString()
      }
      case "INTEGER" => {
        value.asInt().toString
      }
      case "FLOAT" | "DOUBLE" => {
        value.asDouble().toString
      }
      case "BOOLEAN" => {
        value.asBoolean().toString
      }
      case "DATE" | "LOCAL_DATE" => {
        value.asLocalDate().toString
      }
      case "DATE_TIME" | "LOCAL_DATE_TIME" => {
        value.asLocalDateTime().toString
      }
      case "TIME" | "LOCAL_TIME" => {
        value.asLocalTime().toString
      }
      case "BYTES" => {
        new String(value.asByteArray())
      }
      case "LIST" => {
        value.asList().toString
      }
      case "MAP" => {
        value.asMap().toString
      }
      case _ => {
        value.toString
      }
    }
  }
}
