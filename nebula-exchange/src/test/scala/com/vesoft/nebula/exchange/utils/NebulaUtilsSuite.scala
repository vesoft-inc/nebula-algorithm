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
import com.vesoft.nebula.exchange.config.{NebulaSinkConfigEntry, SinkCategory, TagConfigEntry}
import com.vesoft.nebula.exchange.utils.NebulaUtils
import com.vesoft.nebula.exchange.{KeyPolicy, MetaProvider, VidType}
import com.vesoft.nebula.meta.PropertyType
import org.apache.log4j.Logger
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.com.vesoft.nebula.exchange.NebulaGraphMock

class NebulaUtilsSuite {
  private[this] val LOG = Logger.getLogger(this.getClass)

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
    val label               = "person"
    val dataSinkConfigEntry = NebulaSinkConfigEntry(SinkCategory.SST, List("127.0.0.1:9669"))
    val sourceConfig = TagConfigEntry(label,
                                      null,
                                      dataSinkConfigEntry,
                                      sourceFields,
                                      nebulaFields,
                                      "id",
                                      Some(KeyPolicy.UUID),
                                      1,
                                      1,
                                      Some(""))

    val space   = "test_string"
    val address = new ListBuffer[HostAndPort]()
    address.append(HostAndPort.fromParts("127.0.0.1", 9559))
    val metaProvider = new MetaProvider(address.toList, 6000, 1)

    val map: Map[String, Int] =
      NebulaUtils.getDataSourceFieldType(sourceConfig, space, metaProvider)
    assert(map("col1") == PropertyType.STRING.getValue)
    assert(map("col2") == PropertyType.FIXED_STRING.getValue)
    assert(map("col3") == PropertyType.INT8.getValue)
    assert(map("col4") == PropertyType.INT16.getValue)
    assert(map("col5") == PropertyType.INT32.getValue)
    assert(map("col6") == PropertyType.INT64.getValue)
    assert(map("col7") == PropertyType.DATE.getValue)
    assert(map("col8") == PropertyType.DATETIME.getValue)
    assert(map("col9") == PropertyType.TIMESTAMP.getValue)
    assert(map("col10") == PropertyType.BOOL.getValue)
    assert(map("col11") == PropertyType.DOUBLE.getValue)
    assert(map("col12") == PropertyType.FLOAT.getValue)
  }

  @Test
  def getPartitionId(): Unit = {
    // for String type vid
    assert(NebulaUtils.getPartitionId("1", 10, VidType.STRING) == 6)
    assert(NebulaUtils.getPartitionId("2", 10, VidType.STRING) == 1)
    assert(NebulaUtils.getPartitionId("3", 10, VidType.STRING) == 4)
    assert(NebulaUtils.getPartitionId("4", 10, VidType.STRING) == 7)
    assert(NebulaUtils.getPartitionId("5", 10, VidType.STRING) == 10)
    assert(NebulaUtils.getPartitionId("6", 10, VidType.STRING) == 2)
    assert(NebulaUtils.getPartitionId("7", 10, VidType.STRING) == 3)
    assert(NebulaUtils.getPartitionId("8", 10, VidType.STRING) == 7)
    assert(NebulaUtils.getPartitionId("9", 10, VidType.STRING) == 5)
    assert(NebulaUtils.getPartitionId("10", 10, VidType.STRING) == 4)
    assert(NebulaUtils.getPartitionId("11", 10, VidType.STRING) == 9)
    assert(NebulaUtils.getPartitionId("12", 10, VidType.STRING) == 4)
    assert(NebulaUtils.getPartitionId("-1", 10, VidType.STRING) == 1)
    assert(NebulaUtils.getPartitionId("-2", 10, VidType.STRING) == 6)
    assert(NebulaUtils.getPartitionId("-3", 10, VidType.STRING) == 1)
    assert(NebulaUtils.getPartitionId("19", 10, VidType.STRING) == 9)
    assert(NebulaUtils.getPartitionId("22", 10, VidType.STRING) == 8)

    // for int type vid
    assert(NebulaUtils.getPartitionId("1", 10, VidType.INT) == 2)
    assert(NebulaUtils.getPartitionId("2", 10, VidType.INT) == 3)
    assert(NebulaUtils.getPartitionId("3", 10, VidType.INT) == 4)
    assert(NebulaUtils.getPartitionId("4", 10, VidType.INT) == 5)
    assert(NebulaUtils.getPartitionId("5", 10, VidType.INT) == 6)
    assert(NebulaUtils.getPartitionId("6", 10, VidType.INT) == 7)
    assert(NebulaUtils.getPartitionId("7", 10, VidType.INT) == 8)
    assert(NebulaUtils.getPartitionId("8", 10, VidType.INT) == 9)
    assert(NebulaUtils.getPartitionId("9", 10, VidType.INT) == 10)
    assert(NebulaUtils.getPartitionId("10", 10, VidType.INT) == 1)
    assert(NebulaUtils.getPartitionId("11", 10, VidType.INT) == 2)
    assert(NebulaUtils.getPartitionId("12", 10, VidType.INT) == 3)
    assert(NebulaUtils.getPartitionId("-1", 10, VidType.INT) == 6)
    assert(NebulaUtils.getPartitionId("-2", 10, VidType.INT) == 5)
    assert(NebulaUtils.getPartitionId("-3", 10, VidType.INT) == 4)
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
    val escapeName = NebulaUtils.escapePropName(fields.toList)
    assert("`col1`".equals(escapeName.head))
    assert("`col2`".equals(escapeName.tail.head))
    assert("`col3`".equals(escapeName.tail.tail.head))
  }
}
