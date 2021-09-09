/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.KeyPolicy
import com.vesoft.nebula.connector.connector.{NebulaEdge, NebulaEdges, NebulaVertex, NebulaVertices}
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

  test("test toExecuteSentence for vertex with hash policy") {
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
    vertices.append(NebulaVertex("vid1", props1))
    vertices.append(NebulaVertex("vid2", props2))

    val nebulaVertices  = NebulaVertices(propNames, vertices.toList, Some(KeyPolicy.HASH))
    val vertexStatement = NebulaExecutor.toExecuteSentence(tagName, nebulaVertices)

    val expectStatement = "INSERT vertex `person`(`col_string`,`col_fixed_string`,`col_bool`," +
      "`col_int`,`col_int64`,`col_double`,`col_date`) VALUES hash(\"vid1\"): (" + props1.mkString(
      ", ") +
      "), hash(\"vid2\"): (" + props2.mkString(", ") + ")"
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
    val vertices: ListBuffer[NebulaVertex] = new ListBuffer[NebulaVertex]
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")

    val props1 = List("\"name\"", "\"name\"", true, 10, 100L, 1.0, "2021-11-12")
    val props2 = List("\"name2\"", "\"name2\"", false, 11, 101L, 2.0, "2021-11-13")

    vertices.append(NebulaVertex("\"vid1\"", props1))
    vertices.append(NebulaVertex("\"vid2\"", props2))
    val nebulaVertices = NebulaVertices(propNames, vertices.toList, None)

    val updateVertexStatement =
      NebulaExecutor.toUpdateExecuteStatement("person", nebulaVertices)

    val expectVertexUpdate =
      "UPDATE VERTEX ON `person` \"vid1\" SET `col_string`=\"name\",`col_fixed_string`=\"name\"," +
        "`col_bool`=true,`col_int`=10,`col_int64`=100,`col_double`=1.0,`col_date`=2021-11-12;" +
        "UPDATE VERTEX ON `person` \"vid2\" SET `col_string`=\"name2\",`col_fixed_string`=\"name2\"," +
        "`col_bool`=false,`col_int`=11,`col_int64`=101,`col_double`=2.0,`col_date`=2021-11-13"
    assert(expectVertexUpdate.equals(updateVertexStatement))
  }

  test("test toUpdateExecuteSentence for vertex with hash policy") {
    val vertices: ListBuffer[NebulaVertex] = new ListBuffer[NebulaVertex]
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")

    val props1 = List("\"name\"", "\"name\"", true, 10, 100L, 1.0, "2021-11-12")
    val props2 = List("\"name2\"", "\"name2\"", false, 11, 101L, 2.0, "2021-11-13")

    vertices.append(NebulaVertex("vid1", props1))
    vertices.append(NebulaVertex("vid2", props2))
    val nebulaVertices = NebulaVertices(propNames, vertices.toList, Some(KeyPolicy.HASH))

    val updateVertexStatement =
      NebulaExecutor.toUpdateExecuteStatement("person", nebulaVertices)
    val expectVertexUpdate =
      "UPDATE VERTEX ON `person` hash(\"vid1\") SET `col_string`=\"name\",`col_fixed_string`=\"name\"," +
        "`col_bool`=true,`col_int`=10,`col_int64`=100,`col_double`=1.0,`col_date`=2021-11-12;" +
        "UPDATE VERTEX ON `person` hash(\"vid2\") SET `col_string`=\"name2\",`col_fixed_string`=\"name2\"," +
        "`col_bool`=false,`col_int`=11,`col_int64`=101,`col_double`=2.0,`col_date`=2021-11-13"
    assert(expectVertexUpdate.equals(updateVertexStatement))
  }

  test("test toUpdateExecuteSentence for edge") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
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

    val nebulaEdges         = NebulaEdges(propNames, edges.toList, None, None)
    val updateEdgeStatement = NebulaExecutor.toUpdateExecuteStatement("friend", nebulaEdges)
    val expectEdgeUpdate =
      "UPDATE EDGE ON `friend` \"vid1\"->\"vid2\"@1 SET `col_string`=\"Tom\"," +
        "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100," +
        "`col_double`=1.0,`col_date`=2021-11-12;" +
        "UPDATE EDGE ON `friend` \"vid2\"->\"vid1\"@2 SET `col_string`=\"Bob\"," +
        "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,`col_int64`=200," +
        "`col_double`=2.0,`col_date`=2021-05-01"
    assert(expectEdgeUpdate.equals(updateEdgeStatement))
  }

  test("test toUpdateExecuteSentence for edge with hash policy") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val propNames = List("col_string",
                         "col_fixed_string",
                         "col_bool",
                         "col_int",
                         "col_int64",
                         "col_double",
                         "col_date")
    val props1 = List("\"Tom\"", "\"Tom\"", true, 10, 100L, 1.0, "2021-11-12")
    val props2 = List("\"Bob\"", "\"Bob\"", false, 20, 200L, 2.0, "2021-05-01")
    edges.append(NebulaEdge("vid1", "vid2", Some(1L), props1))
    edges.append(NebulaEdge("vid2", "vid1", Some(2L), props2))

    val nebulaEdges =
      NebulaEdges(propNames, edges.toList, Some(KeyPolicy.HASH), Some(KeyPolicy.HASH))
    val updateEdgeStatement = NebulaExecutor.toUpdateExecuteStatement("friend", nebulaEdges)
    val expectEdgeUpdate =
      "UPDATE EDGE ON `friend` hash(\"vid1\")->hash(\"vid2\")@1 SET `col_string`=\"Tom\"," +
        "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100," +
        "`col_double`=1.0,`col_date`=2021-11-12;" +
        "UPDATE EDGE ON `friend` hash(\"vid2\")->hash(\"vid1\")@2 SET `col_string`=\"Bob\"," +
        "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,`col_int64`=200," +
        "`col_double`=2.0,`col_date`=2021-05-01"
    assert(expectEdgeUpdate.equals(updateEdgeStatement))
  }

  test("test toDeleteExecuteStatement for vertex") {
    val vertices: ListBuffer[NebulaVertex] = new ListBuffer[NebulaVertex]
    vertices.append(NebulaVertex("\"vid1\"", List()))
    vertices.append(NebulaVertex("\"vid2\"", List()))

    val nebulaVertices              = NebulaVertices(List(), vertices.toList, None)
    val vertexStatement             = NebulaExecutor.toDeleteExecuteStatement(nebulaVertices)
    val expectVertexDeleteStatement = "DELETE VERTEX \"vid1\",\"vid2\""
    assert(expectVertexDeleteStatement.equals(vertexStatement))
  }

  test("test toDeleteExecuteStatement for vertex with HASH policy") {
    val vertices: ListBuffer[NebulaVertex] = new ListBuffer[NebulaVertex]
    vertices.append(NebulaVertex("vid1", List()))
    vertices.append(NebulaVertex("vid2", List()))

    val nebulaVertices              = NebulaVertices(List(), vertices.toList, Some(KeyPolicy.HASH))
    val vertexStatement             = NebulaExecutor.toDeleteExecuteStatement(nebulaVertices)
    val expectVertexDeleteStatement = "DELETE VERTEX hash(\"vid1\"),hash(\"vid2\")"
    assert(expectVertexDeleteStatement.equals(vertexStatement))
  }

  test("test toDeleteExecuteStatement for edge") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    edges.append(NebulaEdge("\"vid1\"", "\"vid2\"", Some(1L), List()))
    edges.append(NebulaEdge("\"vid2\"", "\"vid1\"", Some(2L), List()))

    val nebulaEdges               = NebulaEdges(List(), edges.toList, None, None)
    val edgeStatement             = NebulaExecutor.toDeleteExecuteStatement("friend", nebulaEdges)
    val expectEdgeDeleteStatement = "DELETE EDGE `friend` \"vid1\"->\"vid2\"@1,\"vid2\"->\"vid1\"@2"
    assert(expectEdgeDeleteStatement.equals(edgeStatement))
  }

  test("test toDeleteExecuteStatement for edge without rank") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    edges.append(NebulaEdge("\"vid1\"", "\"vid2\"", Option.empty, List()))
    edges.append(NebulaEdge("\"vid2\"", "\"vid1\"", Option.empty, List()))

    val nebulaEdges               = NebulaEdges(List(), edges.toList, None, None)
    val edgeStatement             = NebulaExecutor.toDeleteExecuteStatement("friend", nebulaEdges)
    val expectEdgeDeleteStatement = "DELETE EDGE `friend` \"vid1\"->\"vid2\"@0,\"vid2\"->\"vid1\"@0"
    assert(expectEdgeDeleteStatement.equals(edgeStatement))
  }

  test("test toDeleteExecuteStatement for edge with src HASH policy") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    edges.append(NebulaEdge("vid1", "\"vid2\"", Some(1L), List()))
    edges.append(NebulaEdge("vid2", "\"vid1\"", Some(2L), List()))

    val nebulaEdges   = NebulaEdges(List(), edges.toList, Some(KeyPolicy.HASH), None)
    val edgeStatement = NebulaExecutor.toDeleteExecuteStatement("friend", nebulaEdges)
    val expectEdgeDeleteStatement =
      "DELETE EDGE `friend` hash(\"vid1\")->\"vid2\"@1,hash(\"vid2\")->\"vid1\"@2"
    assert(expectEdgeDeleteStatement.equals(edgeStatement))
  }

  test("test toDeleteExecuteStatement for edge with all HASH policy") {
    val edges: ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    edges.append(NebulaEdge("vid1", "vid2", Some(1L), List()))
    edges.append(NebulaEdge("vid2", "vid1", Some(2L), List()))

    val nebulaEdges   = NebulaEdges(List(), edges.toList, Some(KeyPolicy.HASH), Some(KeyPolicy.HASH))
    val edgeStatement = NebulaExecutor.toDeleteExecuteStatement("friend", nebulaEdges)
    val expectEdgeDeleteStatement =
      "DELETE EDGE `friend` hash(\"vid1\")->hash(\"vid2\")@1,hash(\"vid2\")->hash(\"vid1\")@2"
    assert(expectEdgeDeleteStatement.equals(edgeStatement))
  }
}
