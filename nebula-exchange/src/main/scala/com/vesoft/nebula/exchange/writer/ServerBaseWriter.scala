/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.writer

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.google.common.base.Optional
import com.google.common.util.concurrent.{FutureCallback, RateLimiter}
import com.vesoft.nebula.graph.ErrorCode
import com.vesoft.nebula.exchange.config.{
  ConnectionConfigEntry,
  DataBaseConfigEntry,
  RateConfigEntry,
  SchemaConfigEntry,
  Type,
  UserConfigEntry
}
import com.vesoft.nebula.exchange.utils.HDFSUtils
import com.vesoft.nebula.exchange.{
  Edges,
  GraphProvider,
  KeyPolicy,
  TooManyErrorsException,
  Vertices
}
import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._

abstract class ServerBaseWriter extends Writer {
  private[this] val BATCH_INSERT_TEMPLATE               = "INSERT %s `%s`(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE               = "%s: (%s)"
  private[this] val INSERT_VALUE_TEMPLATE_WITH_POLICY   = "%s(\"%s\"): (%s)"
  private[this] val ENDPOINT_TEMPLATE                   = "%s(\"%s\")"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                 = "%s->%s@%d: (%s)"

  def toExecuteSentence(name: String, vertices: Vertices): String = {
    BATCH_INSERT_TEMPLATE.format(
      Type.VERTEX.toString,
      name,
      vertices.propertyNames,
      vertices.values
        .map { vertex =>
          if (vertices.policy.isEmpty) {
            INSERT_VALUE_TEMPLATE.format(vertex.vertexID, vertex.propertyValues)
          } else {
            vertices.policy.get match {
              case KeyPolicy.HASH =>
                INSERT_VALUE_TEMPLATE_WITH_POLICY
                  .format(KeyPolicy.HASH.toString, vertex.vertexID, vertex.propertyValues)
              case KeyPolicy.UUID =>
                INSERT_VALUE_TEMPLATE_WITH_POLICY
                  .format(KeyPolicy.UUID.toString, vertex.vertexID, vertex.propertyValues)
              case _ =>
                throw new IllegalArgumentException(
                  s"invalidate vertex policy ${vertices.policy.get}")
            }
          }
        }
        .mkString(", ")
    )
  }

  def toExecuteSentence(name: String, edges: Edges): String = {
    val values = edges.values
      .map { edge =>
        (for (element <- edge.source.split(","))
          yield {
            val source = edges.sourcePolicy match {
              case Some(KeyPolicy.HASH) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, element)
              case Some(KeyPolicy.UUID) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, element)
              case None =>
                element
              case _ =>
                throw new IllegalArgumentException(
                  s"invalidate source policy ${edges.sourcePolicy.get}")
            }

            val target = edges.targetPolicy match {
              case Some(KeyPolicy.HASH) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge.destination)
              case Some(KeyPolicy.UUID) =>
                ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge.destination)
              case None =>
                edge.destination
              case _ =>
                throw new IllegalArgumentException(
                  s"invalidate target policy ${edges.targetPolicy.get}")
            }

            if (edge.ranking.isEmpty)
              EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                .format(source, target, edge.propertyValues)
            else
              EDGE_VALUE_TEMPLATE.format(source, target, edge.ranking.get, edge.propertyValues)
          }).mkString(", ")

      }
      .mkString(", ")
    BATCH_INSERT_TEMPLATE.format(Type.EDGE.toString, name, edges.propertyNames, values)
  }

  def writeVertices(vertices: Vertices): String

  def writeEdges(edges: Edges): String
}

/**
  * write data into Nebula Graph
  */
class NebulaGraphClientWriter(dataBaseConfigEntry: DataBaseConfigEntry,
                              userConfigEntry: UserConfigEntry,
                              connectionConfigEntry: ConnectionConfigEntry,
                              executionRetry: Int,
                              rateConfig: RateConfigEntry,
                              config: SchemaConfigEntry,
                              graphProvider: GraphProvider)
    extends ServerBaseWriter {
  private val LOG = Logger.getLogger(this.getClass)

  require(
    dataBaseConfigEntry.getGraphAddress.nonEmpty
      && dataBaseConfigEntry.getMetaAddress.nonEmpty
      && dataBaseConfigEntry.space.trim.nonEmpty)
  require(userConfigEntry.user.trim.nonEmpty && userConfigEntry.password.trim.nonEmpty)
  require(connectionConfigEntry.timeout > 0 && connectionConfigEntry.retry > 0)
  require(executionRetry > 0)

  val session     = graphProvider.getGraphClient(userConfigEntry)
  val rateLimiter = RateLimiter.create(rateConfig.limit)

  def prepare(): Unit = {
    val switchResult = graphProvider.switchSpace(session, dataBaseConfigEntry.space)
    if (!switchResult) {
      this.close()
      throw new RuntimeException("Switch Failed")
    }

    LOG.info(s"Connection to ${dataBaseConfigEntry.metaAddresses}")
  }

  override def writeVertices(vertices: Vertices): String = {
    val sentence = toExecuteSentence(config.name, vertices)
    if (rateLimiter.tryAcquire(rateConfig.timeout, TimeUnit.MILLISECONDS)) {
      val result = graphProvider.submit(session, sentence)
      if (result.isSucceeded) {
        return null
      }
      LOG.error(s"write vertex failed for ${result.getErrorMessage}")
    } else {
      LOG.error(s"write vertex failed because write speed is too fast")
    }
    LOG.info(sentence)
    sentence
  }

  override def writeEdges(edges: Edges): String = {
    val sentence = toExecuteSentence(config.name, edges)
    if (rateLimiter.tryAcquire(rateConfig.timeout, TimeUnit.MILLISECONDS)) {
      val result = graphProvider.submit(session, sentence)
      if (result.isSucceeded) {
        return null
      }
      LOG.error(s"write edge failed for ${result.getErrorMessage}")
    } else {
      LOG.error(s"write vertex failed because write speed is too fast")
    }
    LOG.info(sentence)
    sentence
  }

  override def close(): Unit = {
    graphProvider.releaseGraphClient(session)
  }
}

class NebulaWriterCallback(latch: CountDownLatch,
                           batchSuccess: LongAccumulator,
                           batchFailure: LongAccumulator,
                           pathAndOffset: Option[(String, Long)])
    extends FutureCallback[java.util.List[Optional[Integer]]] {

  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  private[this] val DEFAULT_ERROR_TIMES = 16

  override def onSuccess(results: java.util.List[Optional[Integer]]): Unit = {
    if (pathAndOffset.isDefined) {
      if (results.asScala.forall(_.get() == ErrorCode.SUCCEEDED))
        HDFSUtils.saveContent(pathAndOffset.get._1, pathAndOffset.get._2.toString)
      else
        throw new RuntimeException(
          s"Some error code: ${results.asScala.filter(_.get() != ErrorCode.SUCCEEDED).head} appear")
    }
    for (result <- results.asScala) {
      latch.countDown()
      if (result.get() == ErrorCode.SUCCEEDED) {
        batchSuccess.add(1)
      } else {
        LOG.error(s"batch insert error with code ${result.get()}, batch size is ${results.size()}")
        batchFailure.add(1)
      }
    }
  }

  override def onFailure(t: Throwable): Unit = {
    latch.countDown()
    if (batchFailure.value > DEFAULT_ERROR_TIMES) {
      throw TooManyErrorsException("too many errors")
    } else {
      batchFailure.add(1)
    }

    if (pathAndOffset.isDefined) {
      throw new RuntimeException(s"Some error appear")
    }
  }
}

/**
  *
  * @param addresses
  * @param space
  */
class NebulaStorageClientWriter(addresses: List[(String, Int)], space: String)
    extends ServerBaseWriter {

  require(addresses.size != 0)

  def this(host: String, port: Int, space: String) = {
    this(List(host -> port), space)
  }

  override def prepare(): Unit = {}

  override def writeVertices(vertices: Vertices): String = ???

  override def writeEdges(edges: Edges): String = ???

  override def close(): Unit = {}
}
