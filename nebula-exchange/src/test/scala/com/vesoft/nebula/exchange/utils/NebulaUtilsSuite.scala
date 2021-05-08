/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package scala.com.vesoft.nebula.exchange.utils

import com.google.common.net.HostAndPort
import com.vesoft.nebula.client.graph.NebulaPoolConfig
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.client.graph.net.NebulaPool
import com.vesoft.nebula.client.storage.StorageClient
import com.vesoft.nebula.exchange.config.TagConfigEntry
import com.vesoft.nebula.exchange.utils.NebulaUtils
import com.vesoft.nebula.exchange.{KeyPolicy, MetaProvider, VidType}
import com.vesoft.nebula.meta.PropertyType
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.com.vesoft.nebula.exchange.NebulaGraphMock

class NebulaUtilsSuite {
  @transient val nebulaPoolConfig = new NebulaPoolConfig
  @transient val pool: NebulaPool = new NebulaPool
  val address                     = new ListBuffer[HostAddress]()
  address.append(new HostAddress("127.0.0.1", 9669))

  val randAddr = scala.util.Random.shuffle(address)
  pool.init(randAddr.asJava, nebulaPoolConfig)

  @Before
  def setUp(): Unit = {
    val mockData = new NebulaGraphMock
    mockData.mockStringIdGraph()
    mockData.mockIntIdGraph()
    mockData.close()
  }

  @After
  def tearDown(): Unit = {}

  @Test
  def getDataSourceFieldType(): Unit = {
    val nebulaFields = List("col1",
                            "col2",
                            "col3",
                            "col4",
                            "col5",
                            "col6",
                            "col7",
                            "col8",
                            "col9",
                            "col10",
                            "col11",
                            "col12")
    val sourceFields = List("col1",
                            "col2",
                            "col3",
                            "col4",
                            "col5",
                            "col6",
                            "col7",
                            "col8",
                            "col9",
                            "col10",
                            "col11",
                            "col12")
    val label = "person"
    val sourceConfig = new TagConfigEntry(label,
                                          null,
                                          null,
                                          sourceFields,
                                          nebulaFields,
                                          null,
                                          Some(KeyPolicy.UUID),
                                          1,
                                          1,
                                          Some(""))

    val space   = "test_string"
    val address = new ListBuffer[HostAndPort]()
    address.append(HostAndPort.fromParts("127.0.0.1", 9559))
    val metaProvider = new MetaProvider(address.toList)

    val map: Map[String, Int] =
      NebulaUtils.getDataSourceFieldType(sourceConfig, space, metaProvider)
    assert(map("col1") == PropertyType.STRING)
    assert(map("col2") == PropertyType.FIXED_STRING)
    assert(map("col3") == PropertyType.INT8)
    assert(map("col4") == PropertyType.INT16)
    assert(map("col5") == PropertyType.INT32)
    assert(map("col6") == PropertyType.INT64)
    assert(map("col7") == PropertyType.DATE)
    assert(map("col8") == PropertyType.DATETIME)
    assert(map("col9") == PropertyType.TIMESTAMP)
    assert(map("col10") == PropertyType.BOOL)
    assert(map("col11") == PropertyType.DOUBLE)
    assert(map("col12") == PropertyType.FLOAT)
  }

  @Test
  def getPartitionId(): Unit = {
    val storageClient = new StorageClient("127.0.0.1", 9559)
    storageClient.connect()
    for (i <- 1 to 12) {
      val vid            = Integer.toString(i)
      val partitionId    = NebulaUtils.getPartitionId(vid, 10, VidType.STRING)
      val scanResultIter = storageClient.scanVertex("test_string", partitionId, "person")
      var containVertex  = false
      while (scanResultIter.hasNext) {
        val scanResult = scanResultIter.next()
        val map        = scanResult.getVidVertices
        for (value <- map.keySet().asScala if !containVertex) {
          if (value.asString().equals(vid)) {
            containVertex = true
          }
        }
      }
      assert(containVertex)
    }

    for (i <- 1 to 12) {
      val vid            = Integer.toString(i)
      val partitionId    = NebulaUtils.getPartitionId(vid, 10, VidType.INT)
      val scanResultIter = storageClient.scanVertex("test_int", partitionId, "person")
      var containVertex  = false
      while (scanResultIter.hasNext) {
        val scanResult = scanResultIter.next()
        val map        = scanResult.getVidVertices
        for (value <- map.keySet().asScala if !containVertex) {
          if (value.asLong() == vid.toLong) {
            containVertex = true
          }
        }
      }
      assert(containVertex)
    }
  }

  @Test
  def isNumeric(): Unit = {
    assert(NebulaUtils.isNumic("123456"))
    assert(NebulaUtils.isNumic("0123456"))
    assert(NebulaUtils.isNumic("-123456"))
    assert(NebulaUtils.isNumic("000"))
    assert(!NebulaUtils.isNumic("aaa"))
    assert(!NebulaUtils.isNumic("0123aaa"))
    assert(!NebulaUtils.isNumic("123a8"))

  }

  @Test
  def escapePropName(): Unit = {
    val fields = new ListBuffer[String]
    fields.append("col1")
    fields.append("col2")
    fields.append("col3")
    val escapeName = NebulaUtils.escapePropName(fields.toList);
    assert("`col1`".equals(escapeName(0)))
    assert("`col2`".equals(escapeName(1)))
    assert("`col3`".equals(escapeName(2)))
  }
}
