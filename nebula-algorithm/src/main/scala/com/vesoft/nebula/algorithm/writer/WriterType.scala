/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.writer

/**
  *
  * @author 梦境迷离
  * @version 1.0,2023/9/12
  */
sealed trait WriterType {
  self =>
  def stringify: String = self match {
    case WriterType.text   => "text"
    case WriterType.nebula => "nebula"
    case WriterType.csv    => "csv"
  }
}
object WriterType {
  lazy val mapping: Map[String, WriterType] = Map(
    text.stringify   -> text,
    nebula.stringify -> nebula,
    csv.stringify    -> csv
  )
  object text   extends WriterType
  object nebula extends WriterType
  object csv    extends WriterType
}
