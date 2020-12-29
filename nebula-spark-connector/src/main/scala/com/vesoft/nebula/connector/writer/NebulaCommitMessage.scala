/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

case class NebulaCommitMessage(executeStatements: List[String]) extends WriterCommitMessage
