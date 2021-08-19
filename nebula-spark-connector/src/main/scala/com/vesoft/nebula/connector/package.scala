/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.writer.NebulaExecutor
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{
  DataFrame,
  DataFrameReader,
  DataFrameWriter,
  Encoder,
  Encoders,
  Row,
  SaveMode
}

import scala.collection.mutable.ListBuffer

package object connector {

  type Address        = (String, Int)
  type NebulaType     = Int
  type Prop           = List[Any]
  type PropertyNames  = List[String]
  type PropertyValues = List[Any]

  type VertexID           = Long
  type VertexIDSlice      = String
  type NebulaGraphxVertex = (VertexID, PropertyValues)
  type NebulaGraphxEdge   = org.apache.spark.graphx.Edge[(EdgeRank, Prop)]
  type EdgeRank           = Long

  case class NebulaVertex(vertexIDSlice: VertexIDSlice, values: PropertyValues) {
    def propertyValues = values.mkString(", ")

    override def toString: String = {
      s"Vertex ID: ${vertexIDSlice}, Values: ${values.mkString(", ")}"
    }
  }

  case class NebulaVertices(propNames: PropertyNames,
                            values: List[NebulaVertex],
                            policy: Option[KeyPolicy.Value]) {

    def propertyNames: String = NebulaExecutor.escapePropName(propNames).mkString(",")

    override def toString: String = {
      s"Vertices: " +
        s"Property Names: ${propNames.mkString(", ")}" +
        s"Vertex Values: ${values.mkString(", ")} " +
        s"with policy: ${policy}"
    }
  }

  case class NebulaEdge(source: VertexIDSlice,
                        target: VertexIDSlice,
                        rank: Option[EdgeRank],
                        values: PropertyValues) {
    def propertyValues: String = values.mkString(", ")

    override def toString: String = {
      s"Edge: ${source}->${target}@${rank} values: ${propertyValues}"
    }
  }

  case class NebulaEdges(propNames: PropertyNames,
                         values: List[NebulaEdge],
                         sourcePolicy: Option[KeyPolicy.Value],
                         targetPolicy: Option[KeyPolicy.Value]) {
    def propertyNames: String = NebulaExecutor.escapePropName(propNames).mkString(",")
    def getSourcePolicy       = sourcePolicy
    def getTargetPolicy       = targetPolicy

    override def toString: String = {
      "Edges:" +
        s" Property Names: ${propNames.mkString(", ")}" +
        s" with source policy ${sourcePolicy}" +
        s" with target policy ${targetPolicy}"
    }
  }

  /**
    * spark reader for nebula graph
    */
  implicit class NebulaDataFrameReader(reader: DataFrameReader) {
    var connectionConfig: NebulaConnectionConfig = _
    var readConfig: ReadNebulaConfig             = _

    def nebula(connectionConfig: NebulaConnectionConfig,
               readConfig: ReadNebulaConfig): NebulaDataFrameReader = {
      this.connectionConfig = connectionConfig
      this.readConfig = readConfig
      this
    }

    /**
      * Reading com.vesoft.nebula.tools.connector.vertices from Nebula Graph
      * @return DataFrame
      */
    def loadVerticesToDF(): DataFrame = {
      assert(connectionConfig != null && readConfig != null,
             "nebula config is not set, please call nebula() before loadVerticesToDF")
      reader
        .format(classOf[NebulaDataSource].getName)
        .option(NebulaOptions.TYPE, DataTypeEnum.VERTEX.toString)
        .option(NebulaOptions.SPACE_NAME, readConfig.getSpace)
        .option(NebulaOptions.LABEL, readConfig.getLabel)
        .option(NebulaOptions.PARTITION_NUMBER, readConfig.getPartitionNum)
        .option(NebulaOptions.RETURN_COLS, readConfig.getReturnCols.mkString(","))
        .option(NebulaOptions.NO_COLUMN, readConfig.getNoColumn)
        .option(NebulaOptions.LIMIT, readConfig.getLimit)
        .option(NebulaOptions.META_ADDRESS, connectionConfig.getMetaAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.CONNECTION_RETRY, connectionConfig.getConnectionRetry)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .load()
    }

    /**
      * Reading edges from Nebula Graph
      * @return DataFrame
      */
    def loadEdgesToDF(): DataFrame = {
      assert(connectionConfig != null && readConfig != null,
             "nebula config is not set, please call nebula() before loadEdgesToDF")

      reader
        .format(classOf[NebulaDataSource].getName)
        .option(NebulaOptions.TYPE, DataTypeEnum.EDGE.toString)
        .option(NebulaOptions.SPACE_NAME, readConfig.getSpace)
        .option(NebulaOptions.LABEL, readConfig.getLabel)
        .option(NebulaOptions.RETURN_COLS, readConfig.getReturnCols.mkString(","))
        .option(NebulaOptions.NO_COLUMN, readConfig.getNoColumn)
        .option(NebulaOptions.LIMIT, readConfig.getLimit)
        .option(NebulaOptions.PARTITION_NUMBER, readConfig.getPartitionNum)
        .option(NebulaOptions.META_ADDRESS, connectionConfig.getMetaAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.CONNECTION_RETRY, connectionConfig.getConnectionRetry)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .load()
    }

    /**
      * read nebula vertex edge to graphx's vertex
      * use hash() for String type vertex id.
      */
    def loadVerticesToGraphx(): RDD[NebulaGraphxVertex] = {
      val vertexDataset = loadVerticesToDF()
      implicit val encoder: Encoder[NebulaGraphxVertex] =
        Encoders.bean[NebulaGraphxVertex](classOf[NebulaGraphxVertex])

      vertexDataset
        .map(row => {
          val vertexId               = row.get(0)
          val vid: Long              = vertexId.toString.toLong
          val props: ListBuffer[Any] = ListBuffer()
          for (i <- row.schema.fields.indices) {
            if (i != 0) {
              props.append(row.get(i))
            }
          }
          (vid, props.toList)
        })(encoder)
        .rdd
    }

    /**
      * read nebula edge edge to graphx's edge
      * use hash() for String type srcId and dstId.
      */
    def loadEdgesToGraphx(): RDD[NebulaGraphxEdge] = {
      val edgeDataset = loadEdgesToDF()
      implicit val encoder: Encoder[NebulaGraphxEdge] =
        Encoders.bean[NebulaGraphxEdge](classOf[NebulaGraphxEdge])

      edgeDataset
        .map(row => {
          val props: ListBuffer[Any] = ListBuffer()
          for (i <- row.schema.fields.indices) {
            if (i != 0 && i != 1 && i != 2) {
              props.append(row.get(i))
            }
          }
          val srcId    = row.get(0)
          val dstId    = row.get(1)
          val edgeSrc  = srcId.toString.toLong
          val edgeDst  = dstId.toString.toLong
          val edgeProp = (row.get(2).toString.toLong, props.toList)
          org.apache.spark.graphx
            .Edge(edgeSrc, edgeDst, edgeProp)
        })(encoder)
        .rdd
    }

  }

  /**
    * spark writer for nebula graph
    */
  implicit class NebulaDataFrameWriter(writer: DataFrameWriter[Row]) {

    var connectionConfig: NebulaConnectionConfig = _
    var writeNebulaConfig: WriteNebulaConfig     = _

    /**
      * config nebula connection
      * @param connectionConfig connection parameters
      * @param writeNebulaConfig write parameters for vertex or edge
      */
    def nebula(connectionConfig: NebulaConnectionConfig,
               writeNebulaConfig: WriteNebulaConfig): NebulaDataFrameWriter = {
      this.connectionConfig = connectionConfig
      this.writeNebulaConfig = writeNebulaConfig
      this
    }

    /**
      * write dataframe into nebula vertex
      */
    def writeVertices(): Unit = {
      assert(connectionConfig != null && writeNebulaConfig != null,
             "nebula config is not set, please call nebula() before writeVertices")
      val writeConfig = writeNebulaConfig.asInstanceOf[WriteNebulaVertexConfig]
      writer
        .format(classOf[NebulaDataSource].getName)
        .mode(SaveMode.Overwrite)
        .option(NebulaOptions.TYPE, DataTypeEnum.VERTEX.toString)
        .option(NebulaOptions.SPACE_NAME, writeConfig.getSpace)
        .option(NebulaOptions.LABEL, writeConfig.getTagName)
        .option(NebulaOptions.USER_NAME, writeConfig.getUser)
        .option(NebulaOptions.PASSWD, writeConfig.getPasswd)
        .option(NebulaOptions.VERTEX_FIELD, writeConfig.getVidField)
        .option(NebulaOptions.VID_POLICY, writeConfig.getVidPolicy)
        .option(NebulaOptions.BATCH, writeConfig.getBatch)
        .option(NebulaOptions.VID_AS_PROP, writeConfig.getVidAsProp)
        .option(NebulaOptions.WRITE_MODE, writeConfig.getWriteMode)
        .option(NebulaOptions.META_ADDRESS, connectionConfig.getMetaAddress)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.CONNECTION_RETRY, connectionConfig.getConnectionRetry)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .save()
    }

    /**
      * write dataframe into nebula edge
      */
    def writeEdges(): Unit = {

      assert(connectionConfig != null && writeNebulaConfig != null,
             "nebula config is not set, please call nebula() before writeEdges")
      val writeConfig = writeNebulaConfig.asInstanceOf[WriteNebulaEdgeConfig]
      writer
        .format(classOf[NebulaDataSource].getName)
        .mode(SaveMode.Overwrite)
        .option(NebulaOptions.TYPE, DataTypeEnum.EDGE.toString)
        .option(NebulaOptions.SPACE_NAME, writeConfig.getSpace)
        .option(NebulaOptions.USER_NAME, writeConfig.getUser)
        .option(NebulaOptions.PASSWD, writeConfig.getPasswd)
        .option(NebulaOptions.LABEL, writeConfig.getEdgeName)
        .option(NebulaOptions.SRC_VERTEX_FIELD, writeConfig.getSrcFiled)
        .option(NebulaOptions.DST_VERTEX_FIELD, writeConfig.getDstField)
        .option(NebulaOptions.SRC_POLICY, writeConfig.getSrcPolicy)
        .option(NebulaOptions.DST_POLICY, writeConfig.getDstPolicy)
        .option(NebulaOptions.RANK_FIELD, writeConfig.getRankField)
        .option(NebulaOptions.BATCH, writeConfig.getBatch)
        .option(NebulaOptions.SRC_AS_PROP, writeConfig.getSrcAsProp)
        .option(NebulaOptions.DST_AS_PROP, writeConfig.getDstAsProp)
        .option(NebulaOptions.RANK_AS_PROP, writeConfig.getRankAsProp)
        .option(NebulaOptions.WRITE_MODE, writeConfig.getWriteMode)
        .option(NebulaOptions.META_ADDRESS, connectionConfig.getMetaAddress)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.CONNECTION_RETRY, connectionConfig.getConnectionRetry)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .save()
    }
  }

}
