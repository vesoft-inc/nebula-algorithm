/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange

import com.vesoft.nebula.graph.{AuthResponse, ErrorCode, ExecutionResponse, GraphService}
import org.apache.log4j.Logger

class MockQueryProcessor extends GraphService.Iface {
  private[this] val LOG = Logger.getLogger(this.getClass)

  var queryStatement: Array[Byte]       = null
  var countDownLatchFailOfInsert: Int   = -1
  var countDownLatchFailOfSentence: Int = -1

  def resetLatch(): Unit = {
    resetLatchInsert()
    resetLatchSentence()
  }

  def resetLatchInsert(): Unit   = countDownLatchFailOfInsert = -1
  def resetLatchSentence(): Unit = countDownLatchFailOfSentence = -1

  override def authenticate(username: Array[Byte], password: Array[Byte]): AuthResponse = {
    LOG.info(s"Get login user: ${username}, password: ${password}")
    if (MockConfigs.userConfig.user == username && MockConfigs.userConfig.password == password)
      new AuthResponse(ErrorCode.SUCCEEDED, "SUCCEEDED".getBytes(), 1)
    else
      new AuthResponse(ErrorCode.E_BAD_USERNAME_PASSWORD, "BAD USERNAME OR PASSWORD".getBytes(), 1)
  }
  override def signout(sessionId: Long): Unit = {}
  override def execute(sessionId: Long, stmt: Array[Byte]): ExecutionResponse = {
    queryStatement = stmt
    if (queryStatement.contains("INSERT")) {
      if (countDownLatchFailOfInsert == 0) {
        LOG.info(s"mock server got statement: ${queryStatement}, return error")
        new ExecutionResponse(ErrorCode.E_SYNTAX_ERROR, 1)
      } else {
        if (countDownLatchFailOfInsert > 0)
          countDownLatchFailOfInsert -= 1
        LOG.info(s"mock server got statement: ${queryStatement}, return success")
        new ExecutionResponse(ErrorCode.SUCCEEDED, 1);
      }
    } else {
      if (countDownLatchFailOfSentence == 0) {
        LOG.info(s"mock server got statement: ${queryStatement}, return error")
        new ExecutionResponse(ErrorCode.E_SYNTAX_ERROR, 1)
      } else {
        if (countDownLatchFailOfSentence > 0)
          countDownLatchFailOfSentence -= 1
        LOG.info(s"mock server got statement: ${queryStatement}, return success")
        new ExecutionResponse(ErrorCode.SUCCEEDED, 1);
      }
    }
  }

  override def executeJson(sessionId: Long, stmt: Array[Byte]): Array[Byte] = ???
}
