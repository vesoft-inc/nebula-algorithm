/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.KeyPolicy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{
  BooleanType,
  DataTypes,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class NebulaExecutorSuite extends AnyFunSuite with BeforeAndAfterAll {
  var schema: StructType = _
  var row: InternalRow   = _

  override def beforeAll(): Unit = {
    val fields = new ListBuffer[StructField]
    fields.append(DataTypes.createStructField("col1", StringType, false))
    fields.append(DataTypes.createStructField("col2", BooleanType, false))
    fields.append(DataTypes.createStructField("col3", LongType, false))
    schema = new StructType(fields.toArray)

    val values = new ListBuffer[Any]
    values.append("aaa")
    values.append(true)
    values.append(1L)
    row = new GenericInternalRow(values.toArray)
  }

  override def afterAll(): Unit = super.afterAll()

  test("test extraID") {
    val index: Int                      = 1
    val policy: Option[KeyPolicy.Value] = None
    val isVidStringType: Boolean        = true
    val stringId                        = NebulaExecutor.extraID(schema, row, index, policy, isVidStringType)
    assert("\"true\"".equals(stringId))

    // test hash vertexId
    val hashId = NebulaExecutor.extraID(schema, row, index, Some(KeyPolicy.HASH), false)
    assert("true".equals(hashId))
  }

  test("test extraRank") {
    // test correct type for rank
    assert(NebulaExecutor.extraRank(schema, row, 2) == 1)

    // test wrong type for rank
    try {
      NebulaExecutor.extraRank(schema, row, 1)
    } catch {
      case e: java.lang.AssertionError => assert(true)
    }
  }

  test("test vid as prop for assignVertexPropValues ") {
    val fieldTypeMap: Map[String, Integer] = Map("col1" -> 6, "col2" -> 1, "col3" -> 2)
    // test vid as prop
    val props = NebulaExecutor.assignVertexPropValues(schema, row, 0, true, fieldTypeMap)
    assert(props.size == 3)
    assert(props.contains("\"aaa\""))
  }

  test("test vid not as prop for assignVertexPropValues ") {
    val fieldTypeMap: Map[String, Integer] = Map("col1" -> 6, "col2" -> 1, "col3" -> 2)
    // test vid not as prop
    val props = NebulaExecutor.assignVertexPropValues(schema, row, 0, false, fieldTypeMap)
    assert(props.size == 2)
    assert(!props.contains("\"aaa\""))
  }

  test("test src & dst & rank all as prop for assignEdgeValues") {
    val fieldTypeMap: Map[String, Integer] = Map("col1" -> 6, "col2" -> 1, "col3" -> 2)

    val prop = NebulaExecutor.assignEdgeValues(schema, row, 0, 1, 2, true, true, true, fieldTypeMap)
    assert(prop.size == 3)
  }

  test("test src & dst & rank all not as prop for assignEdgeValues") {
    val fieldTypeMap: Map[String, Integer] = Map("col1" -> 6, "col2" -> 1, "col3" -> 2)

    val prop =
      NebulaExecutor.assignEdgeValues(schema, row, 0, 1, 2, false, false, false, fieldTypeMap)
    assert(prop.isEmpty)
  }

  test("test assignVertexPropNames") {
    // test vid as prop
    val propNames = NebulaExecutor.assignVertexPropNames(schema, 0, true)
    assert(propNames.size == 3)
    assert(propNames.contains("col1") && propNames.contains("col2") && propNames.contains("col3"))

    // test vid not as prop
    val propNames1 = NebulaExecutor.assignVertexPropNames(schema, 0, false)
    assert(propNames1.size == 2)
    assert(!propNames1.contains("col1") && propNames.contains("col2") && propNames.contains("col3"))
  }

  test("test assignEdgePropNames") {
    // test src / dst / rank all as prop
    val propNames = NebulaExecutor.assignEdgePropNames(schema, 0, 1, 2, true, true, true)
    assert(propNames.size == 3)

    // test src / dst / rank all not as prop
    val propNames1 = NebulaExecutor.assignEdgePropNames(schema, 0, 1, 2, false, false, false)
    assert(propNames1.isEmpty)
  }

}
