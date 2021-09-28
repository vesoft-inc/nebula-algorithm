/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.NebulaTemplate.{
  BATCH_INSERT_TEMPLATE,
  DELETE_EDGE_TEMPLATE,
  DELETE_VERTEX_TEMPLATE,
  EDGE_ENDPOINT_TEMPLATE,
  EDGE_VALUE_TEMPLATE,
  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE,
  ENDPOINT_TEMPLATE,
  UPDATE_EDGE_TEMPLATE,
  UPDATE_VALUE_TEMPLATE,
  UPDATE_VERTEX_TEMPLATE,
  VERTEX_VALUE_TEMPLATE,
  VERTEX_VALUE_TEMPLATE_WITH_POLICY
}
import com.vesoft.nebula.connector.connector.{
  EdgeRank,
  NebulaEdge,
  NebulaEdges,
  NebulaVertex,
  NebulaVertices,
  PropertyNames,
  PropertyValues
}
import com.vesoft.nebula.connector.{DataTypeEnum, KeyPolicy, NebulaUtils}
import com.vesoft.nebula.meta.PropertyType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer

object NebulaExecutor {

  /**
    * deal with vertex ID, used to extra vertex's id and edge's srcId,dstId
    * @param schema
    * @param record
    * @param index
    * @param policy
    * @param isVidStringType true if vid_type is Fix_String
    */
  def extraID(schema: StructType,
              record: InternalRow,
              index: Int,
              policy: Option[KeyPolicy.Value],
              isVidStringType: Boolean): String = {
    val types = schema.fields.map(field => field.dataType)
    val vid   = record.get(index, types(index)).toString
    if (policy.isEmpty) {
      if (isVidStringType) {
        NebulaUtils.escapeUtil(vid).mkString("\"", "", "\"")
      } else {
        assert(NebulaUtils.isNumic(vid))
        vid
      }
    } else {
      vid
    }
  }

  /**
    * extract rank value for edge
    * @param schema
    * @param record
    * @param rankIndex
    */
  def extraRank(schema: StructType, record: InternalRow, rankIndex: Int): EdgeRank = {
    val types = schema.fields.map(field => field.dataType)
    val rank  = record.get(rankIndex, types(rankIndex)).toString
    assert(NebulaUtils.isNumic(rank), s"rank must be numeric, but your rank is ${rank}")
    rank.toLong
  }

  /**
    * deal with vertex property values
    * @param schema
    * @param record
    * @param vertexIndex
    * @param fieldTypeMap
    * */
  def assignVertexPropValues(schema: StructType,
                             record: InternalRow,
                             vertexIndex: Int,
                             vidAsProp: Boolean,
                             fieldTypeMap: Map[String, Integer]): PropertyValues = {
    val values = for {
      index <- schema.fields.indices
      if vidAsProp || index != vertexIndex
    } yield {
      extraValue(record, schema, index, fieldTypeMap)
    }
    values.toList
  }

  /**
    * deal with edge property values
    * @param schema
    * @param record
    * @param srcIndex
    * @param dstIndex
    * @param rankIndex
    * @param fieldTypeMap
    */
  def assignEdgeValues(schema: StructType,
                       record: InternalRow,
                       srcIndex: Int,
                       dstIndex: Int,
                       rankIndex: Int,
                       srcAsProp: Boolean,
                       dstAsProp: Boolean,
                       rankAsProp: Boolean,
                       fieldTypeMap: Map[String, Integer]): PropertyValues = {
    val values = for {
      index <- schema.fields.indices
      if (srcAsProp || index != srcIndex) && (dstAsProp || index != dstIndex) && (rankAsProp || index != rankIndex)
    } yield {
      extraValue(record, schema, index, fieldTypeMap)
    }
    values.toList
  }

  /**
    * get and convert property value
    *
    * @param record DataFrame internal row
    * @param schema DataFrame schema
    * @param index  the position of row columns
    * @param fieldTypeMap property name -> property datatype in nebula
    */
  private[this] def extraValue(record: InternalRow,
                               schema: StructType,
                               index: Int,
                               fieldTypeMap: Map[String, Integer]): Any = {
    if (record.isNullAt(index)) return null

    val types                  = schema.fields.map(field => field.dataType)
    val propValue              = record.get(index, types(index))
    val propValueTypeClassName = propValue.getClass.getName
    val simpleName = propValueTypeClassName.substring(propValueTypeClassName.lastIndexOf(".") + 1,
                                                      propValueTypeClassName.length)

    val fieldName = schema.fields(index).name
    PropertyType.findByValue(fieldTypeMap(fieldName)) match {
      case PropertyType.STRING | PropertyType.FIXED_STRING =>
        NebulaUtils.escapeUtil(propValue.toString).mkString("\"", "", "\"")
      case PropertyType.DATE     => "date(\"" + propValue + "\")"
      case PropertyType.DATETIME => "datetime(\"" + propValue + "\")"
      case PropertyType.TIME     => "time(\"" + propValue + "\")"
      case PropertyType.TIMESTAMP => {
        if (NebulaUtils.isNumic(propValue.toString)) {
          if (simpleName.equalsIgnoreCase("UTF8String")) propValue.toString
          else propValue
        } else {
          "timestamp(\"" + propValue + "\")"
        }
      }
      case _ => {
        if (simpleName.equalsIgnoreCase("UTF8String")) propValue.toString
        else propValue
      }
    }
  }

  /**
    * deal with vertex property names
    * @param schema
    * @param vertexIndex
    */
  def assignVertexPropNames(schema: StructType,
                            vertexIndex: Int,
                            vidAsProp: Boolean): PropertyNames = {
    val propNames = for {
      index <- schema.indices
      if vidAsProp || index != vertexIndex
    } yield {
      schema.fields(index).name
    }
    propNames.toList
  }

  /**
    * deal with edge property names
    * srcId,dstId and rank is not in properties.
    *
    * @param schema DataFrame schema
    * @param srcIndex srcId's position in DF
    * @param dstIndex dstId's position in DF
    * @param rankIndex rankIndex's position in DF
    */
  def assignEdgePropNames(schema: StructType,
                          srcIndex: Int,
                          dstIndex: Int,
                          rankIndex: Int,
                          srcAsProp: Boolean,
                          dstAsProp: Boolean,
                          rankAsProp: Boolean): PropertyNames = {
    val propNames = for {
      index <- schema.indices
      if (srcAsProp || index != srcIndex) && (dstAsProp || index != dstIndex) && (rankAsProp || index != rankIndex)
    } yield {
      schema.fields(index).name
    }
    propNames.toList
  }

  /**
    * construct insert statement for vertex
    */
  def toExecuteSentence(tagName: String, vertices: NebulaVertices): String = {
    BATCH_INSERT_TEMPLATE.format(
      DataTypeEnum.VERTEX.toString,
      tagName,
      vertices.propertyNames,
      vertices.values
        .map { vertex =>
          if (vertices.policy.isEmpty) {
            VERTEX_VALUE_TEMPLATE.format(vertex.vertexIDSlice, vertex.propertyValues)
          } else {
            vertices.policy.get match {
              case KeyPolicy.HASH =>
                VERTEX_VALUE_TEMPLATE_WITH_POLICY
                  .format(KeyPolicy.HASH.toString, vertex.vertexIDSlice, vertex.propertyValues)
              case KeyPolicy.UUID =>
                VERTEX_VALUE_TEMPLATE_WITH_POLICY
                  .format(KeyPolicy.UUID.toString, vertex.vertexIDSlice, vertex.propertyValues)
              case _ =>
                throw new IllegalArgumentException("Not Support")
            }
          }
        }
        .mkString(", ")
    )
  }

  /**
    * construct insert statement for edge
    */
  def toExecuteSentence(edgeName: String, edges: NebulaEdges): String = {
    val values = edges.values
      .map { edge =>
        val source = edges.getSourcePolicy match {
          case Some(KeyPolicy.HASH) =>
            ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge.source)
          case Some(KeyPolicy.UUID) =>
            ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge.source)
          case None =>
            edge.source
          case _ =>
            throw new IllegalArgumentException(
              s"source policy ${edges.getSourcePolicy.get} is not supported")
        }

        val target = edges.getTargetPolicy match {
          case Some(KeyPolicy.HASH) =>
            ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge.target)
          case Some(KeyPolicy.UUID) =>
            ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge.target)
          case None =>
            edge.target
          case _ =>
            throw new IllegalArgumentException(
              s"target policy ${edges.getTargetPolicy.get} is not supported")
        }

        if (edge.rank.isEmpty)
          EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
            .format(source, target, edge.propertyValues)
        else
          EDGE_VALUE_TEMPLATE.format(source, target, edge.rank.get, edge.propertyValues)
      }
      .mkString(", ")
    BATCH_INSERT_TEMPLATE.format(DataTypeEnum.EDGE.toString, edgeName, edges.propertyNames, values)
  }

  /**
    * construct update statement for vertex
    */
  def toUpdateExecuteStatement(tagName: String, nebulaVertices: NebulaVertices): String = {
    nebulaVertices.values
      .map { vertex =>
        var index = 0
        UPDATE_VERTEX_TEMPLATE.format(
          DataTypeEnum.VERTEX.toString.toUpperCase,
          tagName,
          nebulaVertices.policy match {
            case Some(KeyPolicy.HASH) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, vertex.vertexIDSlice)
            case Some(KeyPolicy.UUID) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, vertex.vertexIDSlice)
            case None =>
              vertex.vertexIDSlice
            case _ =>
              throw new IllegalArgumentException(
                s"vertex id policy ${nebulaVertices.policy.get} is not supported")
          },
          vertex.values
            .map { value =>
              val updateValue =
                UPDATE_VALUE_TEMPLATE.format(nebulaVertices.propNames.get(index), value)
              index += 1
              updateValue
            }
            .mkString(",")
        )
      }
      .mkString(";")
  }

  /**
    * construct update statement for edge
    */
  def toUpdateExecuteStatement(edgeName: String, nebulaEdges: NebulaEdges): String = {

    nebulaEdges.values
      .map { edge =>
        var index = 0
        val rank  = if (edge.rank.isEmpty) { 0 } else { edge.rank.get }
        UPDATE_EDGE_TEMPLATE.format(
          DataTypeEnum.EDGE.toString.toUpperCase,
          edgeName,
          nebulaEdges.getSourcePolicy match {
            case Some(KeyPolicy.HASH) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge.source)
            case Some(KeyPolicy.UUID) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge.source)
            case None =>
              edge.source
            case _ =>
              throw new IllegalArgumentException(
                s"source policy ${nebulaEdges.getTargetPolicy.get} is not supported")
          },
          nebulaEdges.getTargetPolicy match {
            case Some(KeyPolicy.HASH) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge.target)
            case Some(KeyPolicy.UUID) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge.target)
            case None =>
              edge.target
            case _ =>
              throw new IllegalArgumentException(
                s"target policy ${nebulaEdges.getTargetPolicy.get} is not supported")
          },
          rank,
          edge.values
            .map { value =>
              val updateValue =
                UPDATE_VALUE_TEMPLATE.format(nebulaEdges.propNames.get(index), value)
              index += 1
              updateValue
            }
            .mkString(",")
        )
      }
      .mkString(";")
  }

  /**
    * construct delete statement for vertex
    */
  def toDeleteExecuteStatement(vertices: NebulaVertices): String = {
    DELETE_VERTEX_TEMPLATE.format(
      vertices.values
        .map { value =>
          vertices.policy match {
            case Some(KeyPolicy.HASH) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, value.vertexIDSlice)

            case Some(KeyPolicy.UUID) =>
              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, value.vertexIDSlice)

            case None =>
              value.vertexIDSlice
            case _ =>
              throw new IllegalArgumentException(
                s"vertex policy ${vertices.policy.get} is not supported")
          }
        }
        .mkString(",")
    )
  }

  /**
    * construct delete statement for edge
    */
  def toDeleteExecuteStatement(edgeName: String, edges: NebulaEdges): String = {
    DELETE_EDGE_TEMPLATE.format(
      edgeName,
      edges.values
        .map { value =>
          EDGE_ENDPOINT_TEMPLATE.format(
            edges.getSourcePolicy match {
              case Some(KeyPolicy.HASH) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, value.source)
              case Some(KeyPolicy.UUID) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, value.source)
              case None => value.source
              case _ =>
                throw new IllegalArgumentException(
                  s"source vertex policy ${edges.getSourcePolicy.get} is not supported")
            },
            edges.getTargetPolicy match {
              case Some(KeyPolicy.HASH) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, value.target)
              case Some(KeyPolicy.UUID) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, value.target)
              case None => value.target
              case _ =>
                throw new IllegalArgumentException(
                  s"target vertex policy ${edges.getTargetPolicy.get} is not supported")
            },
            if (value.rank.isEmpty) 0 else value.rank.get
          )
        }
        .mkString(",")
    )
  }

  /**
    * escape nebula property name, add `` for each property.
    *
    * @param nebulaFields nebula property name list
    * @return escaped nebula property name list
    */
  def escapePropName(nebulaFields: List[String]): List[String] = {
    val propNames: ListBuffer[String] = new ListBuffer[String]
    for (key <- nebulaFields) {
      val sb = new StringBuilder()
      sb.append("`")
      sb.append(key)
      sb.append("`")
      propNames.append(sb.toString())
    }
    propNames.toList
  }

}
