/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.KeyPolicy
import com.vesoft.nebula.connector.connector.{NebulaEdge, NebulaEdges, NebulaVertex, NebulaVertices}
import org.apache.spark.api.java.Optional
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

  test("test toExecuteSentence for vertex") {
    val vertices: ListBuffer[NebulaVertex] = new ListBuffer[NebulaVertex]
    val tagName                            = "person"
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")

    val props1 = List("\"Tom\"", "\"Tom\"", true, 10, 100L, 1.0, "2021-11-12")
    val props2 = List("\"Bob\"", "\"Bob\"", false, 20, 200L, 2.0, "2021-05-01")
    vertices.append(NebulaVertex("\"vid1\"", props1))
    vertices.append(NebulaVertex("\"vid2\"", props2))

    val nebulaVertices  = NebulaVertices(propNames, vertices.toList, None)
    val vertexStatement = NebulaExecutor.toExecuteSentence(tagName, nebulaVertices)

    val expectStatement = "INSERT vertex `person`(`col_string`,`col_fixed_string`,`col_bool`," +
      "`col_int`,`col_int64`,`col_double`,`col_date`) VALUES \"vid1\": (" + props1.mkString(", ") +
      "), \"vid2\": (" + props2.mkString(", ") + ")"
    assert(expectStatement.equals(vertexStatement))
  }

  test("test toExecuteSentence for edge") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeName                      = "friend"
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")
    val props1 = List("\"Tom\"", "\"Tom\"", true, 10, 100L, 1.0, "2021-11-12")
    val props2 = List("\"Bob\"", "\"Bob\"", false, 20, 200L, 2.0, "2021-05-01")
    edges.append(NebulaEdge("\"vid1\"", "\"vid2\"", Some(1L), props1))
    edges.append(NebulaEdge("\"vid2\"", "\"vid1\"", Some(2L), props2))

    val nebulaEdges   = NebulaEdges(propNames, edges.toList, None, None)
    val edgeStatement = NebulaExecutor.toExecuteSentence(edgeName, nebulaEdges)

    val expectStatement = "INSERT edge `friend`(`col_string`,`col_fixed_string`,`col_bool`,`col_int`" +
      ",`col_int64`,`col_double`,`col_date`) VALUES \"vid1\"->\"vid2\"@1: (" + props1.mkString(", ") +
      "), \"vid2\"->\"vid1\"@2: (" + props2.mkString(", ") + ")"
    assert(expectStatement.equals(edgeStatement))
  }

  test("test toUpdateExecuteSentence for vertex") {
    val props = List("\"name\"", "\"name\"", true, 10, 100L, 1.0, "2021-11-12")
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")

    val vertexId     = "\"vid1\""
    val nebulaVertex = NebulaVertex(vertexId, props)
    val updateVertexStatement =
      NebulaExecutor.toUpdateExecuteStatement("person", propNames, nebulaVertex)
    val expectVertexUpdate =
      "UPDATE VERTEX ON `person` \"vid1\" SET `col_string`=\"name\",`col_fixed_string`=\"name\"," +
        "`col_bool`=true,`col_int`=10,`col_int64`=100,`col_double`=1.0,`col_date`=2021-11-12"
    assert(expectVertexUpdate.equals(updateVertexStatement))
  }

  test("test toUpdateExecuteSentence for edge") {
    val props = List("\"name\"", "\"name\"", true, 10, 100L, 1.0, "2021-11-12")
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")

    val source     = "\"source\""
    val target     = "\"target\""
    val rank       = Some(0L)
    val nebulaEdge = NebulaEdge(source, target, rank, props)
    val updateEdgeStatement =
      NebulaExecutor.toUpdateExecuteStatement("friend", propNames, nebulaEdge)
    val expectEdgeUpdate =
      "UPDATE EDGE ON `friend` \"source\"->\"target\"@0 SET `col_string`=\"name\"," +
        "`col_fixed_string`=\"name\",`col_bool`=true,`col_int`=10,`col_int64`=100," +
        "`col_double`=1.0,`col_date`=2021-11-12"
    assert(expectEdgeUpdate.equals(updateEdgeStatement))
  }
}
