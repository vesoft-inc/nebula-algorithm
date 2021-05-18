/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 *  This source code is licensed under Apache 2.0 License,
 *  attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NebulaConfigSuite extends AnyFunSuite with BeforeAndAfterAll {

  test("test NebulaConnectionConfig") {
    try {
      NebulaConnectionConfig.builder().withTimeout(1).build()
    } catch {
      case e: java.lang.AssertionError => assert(true)
    }

    try {
      NebulaConnectionConfig.builder().withTimeout(-1).build()
    } catch {
      case e: java.lang.AssertionError => assert(true)
    }

    try {
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withTimeout(1)
        .build()
      assert(true)
    } catch {
      case _: Throwable => assert(false)
    }
  }

  test("test WriteNebulaConfig") {
    var writeNebulaConfig: WriteNebulaVertexConfig = null
    try {
      writeNebulaConfig = WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vid")
        .build()
    } catch {
      case e: Throwable => assert(false)
    }
    assert(true)
    assert(!writeNebulaConfig.getVidAsProp)
    assert(writeNebulaConfig.getSpace.equals("test"))
  }

  test("test wrong policy") {
    try {
      WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vId")
        .withVidPolicy("wrong_policy")
        .build()
    } catch {
      case e: java.lang.AssertionError => assert(true)
    }
  }

  test("test wrong batch") {
    try {
      WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vId")
        .withVidPolicy("hash")
        .withBatch(-1)
        .build()
    } catch {
      case e: java.lang.AssertionError => assert(true)
    }
  }

  test("test ReadNebulaConfig") {
    try {
      ReadNebulaConfig
        .builder()
        .withSpace("test")
        .withLabel("tagName")
        .withNoColumn(true)
        .withReturnCols(List("col"))
        .build()
    } catch {
      case e: java.lang.AssertionError => assert(false)
    }
  }

}
