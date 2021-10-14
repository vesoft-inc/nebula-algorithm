/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.utils

import org.junit.Test

class NebulaUtilSuite {

  @Test
  def validateWithWeight: Unit = {
    val hostPorts: String        = "127.0.0.1:9559"
    val nameSpace: String        = "nb"
    val labels: List[String]     = List("serve", "follow")
    val hasWeight: Boolean       = true
    val weightCols: List[String] = List("start_year", "degree")
  }

  @Test
  def validateWithoutWeight: Unit = {
    val hostPorts: String        = "127.0.0.1:9559"
    val nameSpace: String        = "nb"
    val labels: List[String]     = List("serve")
    val hasWeight: Boolean       = false
    val weightCols: List[String] = List()
  }

  @Test
  def getResultPathWithEnding: Unit = {
    val path: String          = "/tmp/"
    val algorithmName: String = "aaa"
    assert(NebulaUtil.getResultPath(path, algorithmName).equals("/tmp/aaa"))
  }

  @Test
  def getResultPathWithoutEnding: Unit = {
    val path: String          = "/tmp"
    val algorithmName: String = "aaa"
    assert(NebulaUtil.getResultPath(path, algorithmName).equals("/tmp/aaa"))
  }
}
