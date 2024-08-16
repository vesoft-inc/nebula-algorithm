/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.reader

/**
  *
  * @author 梦境迷离
  * @version 1.0,2023/9/12
  */
sealed trait ReaderType {
  self =>
  def stringify: String = self match {
    case ReaderType.json       => "json"
    case ReaderType.nebulaNgql => "nebula-ngql"
    case ReaderType.nebula     => "nebula"
    case ReaderType.csv        => "csv"
    case ReaderType.hive        => "hive"
  }
}
object ReaderType {
  lazy val mapping: Map[String, ReaderType] = Map(
    json.stringify       -> json,
    nebulaNgql.stringify -> nebulaNgql,
    nebula.stringify     -> nebula,
    csv.stringify        -> csv,
    hive.stringify        -> hive
  )
  object json       extends ReaderType
  object nebulaNgql extends ReaderType
  object nebula     extends ReaderType
  object csv        extends ReaderType
  object hive        extends ReaderType
}
