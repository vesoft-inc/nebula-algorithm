/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import java.util.Properties

import com.google.common.net.HostAndPort
import com.vesoft.nebula.connector.connector.Address
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.mutable.ListBuffer

class NebulaOptions(@transient val parameters: CaseInsensitiveMap[String])(
    operaType: OperaType.Value)
    extends Serializable
    with Logging {

  import NebulaOptions._

  def this(parameters: Map[String, String], operaType: OperaType.Value) =
    this(CaseInsensitiveMap(parameters))(operaType)

  def this(hostAndPorts: String,
           spaceName: String,
           dataType: String,
           label: String,
           parameters: Map[String, String],
           operaType: OperaType.Value) = {
    this(
      CaseInsensitiveMap(
        parameters ++ Map(
          NebulaOptions.META_ADDRESS -> hostAndPorts,
          NebulaOptions.SPACE_NAME   -> spaceName,
          NebulaOptions.TYPE         -> dataType,
          NebulaOptions.LABEL        -> label
        ))
    )(operaType)
  }

  /**
    * Return property with all options
    */
  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  val timeout: Int = parameters.getOrElse(TIMEOUT, DEFAULT_CONNECTION_TIMEOUT).toString.toInt
  val connectionRetry: Int =
    parameters.getOrElse(CONNECTION_RETRY, DEFAULT_CONNECTION_RETRY).toString.toInt
  val executionRetry: Int =
    parameters.getOrElse(EXECUTION_RETRY, DEFAULT_EXECUTION_RETRY).toString.toInt
  val user: String      = parameters.getOrElse(USER_NAME, DEFAULT_USER_NAME)
  val passwd: String    = parameters.getOrElse(PASSWD, DEFAULT_PASSWD)
  val rateLimit: Long   = parameters.getOrElse(RATE_LIMIT, DEFAULT_RATE_LIMIT).toString.toLong
  val rateTimeOut: Long = parameters.getOrElse(RATE_TIME_OUT, DEFAULT_RATE_TIME_OUT).toString.toLong

  require(parameters.isDefinedAt(TYPE), s"Option '$TYPE' is required")
  val dataType: String = parameters(TYPE)
  require(
    DataTypeEnum.validDataType(dataType),
    s"Option '$TYPE' is illegal, it should be '${DataTypeEnum.VERTEX}' or '${DataTypeEnum.EDGE}'")

  /** nebula common parameters */
  require(parameters.isDefinedAt(META_ADDRESS), s"Option '$META_ADDRESS' is required")
  val metaAddress: String = parameters(META_ADDRESS)

  require(parameters.isDefinedAt(SPACE_NAME) && StringUtils.isNotBlank(parameters(SPACE_NAME)),
          s"Option '$SPACE_NAME' is required and can not be blank")
  val spaceName: String = parameters(SPACE_NAME)

  require(parameters.isDefinedAt(LABEL) && StringUtils.isNotBlank(parameters(LABEL)),
          s"Option '$LABEL' is required and can not be blank")
  val label: String = parameters(LABEL)

  /** read parameters */
  var returnCols: String    = _
  var partitionNums: String = _
  var noColumn: Boolean     = _
  var limit: Int            = _
  if (operaType == OperaType.READ) {
    returnCols = parameters(RETURN_COLS)
    noColumn = parameters.getOrElse(NO_COLUMN, false).toString.toBoolean
    partitionNums = parameters(PARTITION_NUMBER)
    limit = parameters.getOrElse(LIMIT, DEFAULT_LIMIT).toString.toInt
  }

  /** write parameters */
  var graphAddress: String       = _
  var vidPolicy: String          = _
  var srcPolicy: String          = _
  var dstPolicy: String          = _
  var vertexField: String        = _
  var srcVertexField: String     = _
  var dstVertexField: String     = _
  var rankField: String          = _
  var batch: Int                 = _
  var vidAsProp: Boolean         = _
  var srcAsProp: Boolean         = _
  var dstAsProp: Boolean         = _
  var rankAsProp: Boolean        = _
  var writeMode: WriteMode.Value = _

  if (operaType == OperaType.WRITE) {
    require(parameters.isDefinedAt(GRAPH_ADDRESS),
            s"option $GRAPH_ADDRESS is required and can not be blank")
    graphAddress = parameters(GRAPH_ADDRESS)

    if (parameters.isDefinedAt(VID_POLICY)) {
      vidPolicy = parameters(VID_POLICY)
    } else {
      vidPolicy = null
    }

    if (parameters.isDefinedAt(SRC_POLICY)) {
      srcPolicy = parameters(SRC_POLICY)
    } else {
      srcPolicy = null
    }

    if (parameters.isDefinedAt(DST_POLICY)) {
      dstPolicy = parameters(DST_POLICY)
    } else {
      dstPolicy = null
    }

    vertexField = parameters.getOrElse(VERTEX_FIELD, null)
    srcVertexField = parameters.getOrElse(SRC_VERTEX_FIELD, null)
    dstVertexField = parameters.getOrElse(DST_VERTEX_FIELD, null)
    rankField = parameters.getOrElse(RANK_FIELD, null)
    batch = parameters.getOrElse(BATCH, DEFAULT_BATCH).toString.toInt
    vidAsProp = parameters.getOrElse(VID_AS_PROP, false).toString.toBoolean
    srcAsProp = parameters.getOrElse(SRC_AS_PROP, false).toString.toBoolean
    dstAsProp = parameters.getOrElse(DST_AS_PROP, false).toString.toBoolean
    rankAsProp = parameters.getOrElse(RANK_AS_PROP, false).toString.toBoolean
    writeMode =
      WriteMode.withName(parameters.getOrElse(WRITE_MODE, DEFAULT_WRITE_MODE).toString.toLowerCase)
  }

  def getReturnCols: List[String] = {
    if (returnCols.trim.isEmpty) {
      List()
    } else {
      returnCols.split(",").toList
    }
  }

  def getMetaAddress: List[Address] = {
    val hostPorts: ListBuffer[Address] = new ListBuffer[Address]
    metaAddress
      .split(",")
      .foreach(hostPort => {
        // check host & port by getting HostAndPort
        val addr = HostAndPort.fromString(hostPort)
        hostPorts.append((addr.getHostText, addr.getPort))
      })
    hostPorts.toList
  }

  def getGraphAddress: List[Address] = {
    val hostPorts: ListBuffer[Address] = new ListBuffer[Address]
    graphAddress
      .split(",")
      .foreach(hostPort => {
        // check host & port by getting HostAndPort
        val addr = HostAndPort.fromString(hostPort)
        hostPorts.append((addr.getHostText, addr.getPort))
      })
    hostPorts.toList
  }

}

class NebulaOptionsInWrite(@transient override val parameters: CaseInsensitiveMap[String])
    extends NebulaOptions(parameters)(OperaType.WRITE) {}

object NebulaOptions {

  /** nebula common config */
  val SPACE_NAME: String    = "spaceName"
  val META_ADDRESS: String  = "metaAddress"
  val GRAPH_ADDRESS: String = "graphAddress"
  val TYPE: String          = "type"
  val LABEL: String         = "label"

  /** connection config */
  val TIMEOUT: String          = "timeout"
  val CONNECTION_RETRY: String = "connectionRetry"
  val EXECUTION_RETRY: String  = "executionRetry"
  val RATE_TIME_OUT: String    = "reteTimeOut"
  val USER_NAME: String        = "user"
  val PASSWD: String           = "passwd"

  /** read config */
  val RETURN_COLS: String      = "returnCols"
  val NO_COLUMN: String        = "noColumn"
  val PARTITION_NUMBER: String = "partitionNumber"
  val LIMIT: String            = "limit"

  /** write config */
  val RATE_LIMIT: String   = "rateLimit"
  val VID_POLICY: String   = "vidPolicy"
  val SRC_POLICY: String   = "srcPolicy"
  val DST_POLICY: String   = "dstPolicy"
  val VERTEX_FIELD         = "vertexField"
  val SRC_VERTEX_FIELD     = "srcVertexField"
  val DST_VERTEX_FIELD     = "dstVertexField"
  val RANK_FIELD           = "rankFiled"
  val BATCH: String        = "batch"
  val VID_AS_PROP: String  = "vidAsProp"
  val SRC_AS_PROP: String  = "srcAsProp"
  val DST_AS_PROP: String  = "dstAsProp"
  val RANK_AS_PROP: String = "rankAsProp"
  val WRITE_MODE: String   = "writeMode"

  val DEFAULT_TIMEOUT: Int            = 3000
  val DEFAULT_CONNECTION_TIMEOUT: Int = 3000
  val DEFAULT_CONNECTION_RETRY: Int   = 3
  val DEFAULT_EXECUTION_RETRY: Int    = 3
  val DEFAULT_USER_NAME: String       = "root"
  val DEFAULT_PASSWD: String          = "nebula"

  val DEFAULT_LIMIT: Int = 1000

  val DEFAULT_RATE_LIMIT: Long    = 1024L
  val DEFAULT_RATE_TIME_OUT: Long = 100
  val DEFAULT_POLICY: String      = null
  val DEFAULT_BATCH: Int          = 1000

  val DEFAULT_WRITE_MODE = WriteMode.INSERT

  val EMPTY_STRING: String = ""
}
