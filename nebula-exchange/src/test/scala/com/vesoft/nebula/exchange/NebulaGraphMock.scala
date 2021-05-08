/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package scala.com.vesoft.nebula.exchange

import com.vesoft.nebula.client.graph.NebulaPoolConfig
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.client.graph.net.NebulaPool

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class NebulaGraphMock {

  @transient val nebulaPoolConfig = new NebulaPoolConfig
  @transient val pool: NebulaPool = new NebulaPool
  val address                     = new ListBuffer[HostAddress]()
  address.append(new HostAddress("127.0.0.1", 9669))

  val randAddr = scala.util.Random.shuffle(address)
  pool.init(randAddr.asJava, nebulaPoolConfig)

  def mockStringIdGraph(): Unit = {
    val session = pool.getSession("root", "nebula", true)

    val createSpace = "CREATE SPACE IF NOT EXISTS test_string(partition_num=10);" +
      "USE test_string;" + "CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16, col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double, col12 float, col13 time);" +
      "CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16, col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double, col12 float, col13 time);";
    val createResp = session.execute(createSpace)
    if (!createResp.isSucceeded) {
      close()
      sys.exit(-1)
    }

    Thread.sleep(3000)
    val insertTag =
      "INSERT VERTEX person(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13) VALUES " +
        " \"1\":(\"person1\", \"person1\", 11, 200, 1000, 188888, date(\"2021-01-01\"), datetime(\"2021-01-01T12:00:00\"),timestamp(\"2021-01-01T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"2\":(\"person2\", \"person2\", 12, 300, 2000, 288888, date(\"2021-01-02\"), datetime(\"2021-01-02T12:00:00\"),timestamp(\"2021-01-02T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"3\":(\"person3\", \"person3\", 13, 400, 3000, 388888, date(\"2021-01-03\"), datetime(\"2021-01-03T12:00:00\"),timestamp(\"2021-01-03T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"4\":(\"person4\", \"person4\", 14, 500, 4000, 488888, date(\"2021-01-04\"), datetime(\"2021-01-04T12:00:00\"),timestamp(\"2021-01-04T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"5\":(\"person5\", \"person5\", 15, 600, 5000, 588888, date(\"2021-01-05\"), datetime(\"2021-01-05T12:00:00\"),timestamp(\"2021-01-05T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"6\":(\"person6\", \"person6\", 16, 700, 6000, 688888, date(\"2021-01-06\"), datetime(\"2021-01-06T12:00:00\"),timestamp(\"2021-01-06T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"7\":(\"person7\", \"person7\", 17, 800, 7000, 788888, date(\"2021-01-07\"), datetime(\"2021-01-07T12:00:00\"),timestamp(\"2021-01-07T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"8\":(\"person8\", \"person8\", 18, 900, 8000, 888888, date(\"2021-01-08\"), datetime(\"2021-01-08T12:00:00\"),timestamp(\"2021-01-08T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"9\":(\"person9\", \"person9\", 19, 1000, 9000, 988888, date(\"2021-01-09\"), datetime(\"2021-01-09T12:00:00\"),timestamp(\"2021-01-09T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"10\":(\"person10\", \"person10\", 20, 1100, 10000, 1088888, date(\"2021-01-10\"), datetime(\"2021-01-10T12:00:00\"),timestamp(\"2021-01-10T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"11\":(\"person11\", \"person11\", 21, 1200, 11000, 1188888, date(\"2021-01-11\"), datetime(\"2021-01-11T12:00:00\"),timestamp(\"2021-01-11T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"12\":(\"person12\", \"person11\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"-1\":(\"person00\", \"person00\", 23, 1400, 13000, 1388888, date(\"2021-01-13\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"-2\":(\"person01\", \"person01\", 24, 1500, 14000, 1488888, date(\"2021-01-14\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"19\":(\"person19\", \"person22\", 25, 1500, 14000, 1488888, date(\"2021-01-14\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " \"22\":(\"person22\", \"person22\", 26, 1500, 14000, 1488888, date(\"2021-01-14\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))"
    val insertTagResp = session.execute(insertTag)
    if (!insertTagResp.isSucceeded) {
      close()
      sys.exit(-1)
    }

    val insertEdge = "INSERT EDGE friend(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13) VALUES " +
      " \"1\" -> \"2\":(\"friend1\", \"friend2\", 11, 200, 1000, 188888, date(\"2021-01-01\"), datetime(\"2021-01-01T12:00:00\"),timestamp(\"2021-01-01T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"2\" -> \"3\":(\"friend2\", \"friend3\", 12, 300, 2000, 288888, date(\"2021-01-02\"), datetime(\"2021-01-02T12:00:00\"),timestamp(\"2021-01-02T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"3\" -> \"4\":(\"friend3\", \"friend4\", 13, 400, 3000, 388888, date(\"2021-01-03\"), datetime(\"2021-01-03T12:00:00\"),timestamp(\"2021-01-03T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"4\" -> \"5\":(\"friend4\", \"friend4\", 14, 500, 4000, 488888, date(\"2021-01-04\"), datetime(\"2021-01-04T12:00:00\"),timestamp(\"2021-01-04T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"5\" -> \"6\":(\"friend5\", \"friend5\", 15, 600, 5000, 588888, date(\"2021-01-05\"), datetime(\"2021-01-05T12:00:00\"),timestamp(\"2021-01-05T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"6\" -> \"7\":(\"friend6\", \"friend6\", 16, 700, 6000, 688888, date(\"2021-01-06\"), datetime(\"2021-01-06T12:00:00\"),timestamp(\"2021-01-06T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"7\" -> \"8\":(\"friend7\", \"friend7\", 17, 800, 7000, 788888, date(\"2021-01-07\"), datetime(\"2021-01-07T12:00:00\"),timestamp(\"2021-01-07T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"8\" -> \"9\":(\"friend8\", \"friend8\", 18, 900, 8000, 888888, date(\"2021-01-08\"), datetime(\"2021-01-08T12:00:00\"),timestamp(\"2021-01-08T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"9\" -> \"10\":(\"friend9\", \"friend9\", 19, 1000, 9000, 988888, date(\"2021-01-09\"), datetime(\"2021-01-09T12:00:00\"),timestamp(\"2021-01-09T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"10\" -> \"11\":(\"friend10\", \"friend10\", 20, 1100, 10000, 1088888, date(\"2021-01-10\"), datetime(\"2021-01-10T12:00:00\"),timestamp(\"2021-01-10T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"11\" -> \"12\":(\"friend11\", \"friend11\", 21, 1200, 11000, 1188888, date(\"2021-01-11\"), datetime(\"2021-01-11T12:00:00\"),timestamp(\"2021-01-11T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"12\" -> \"1\":(\"friend12\", \"friend11\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"-1\" -> \"11\":(\"friend13\", \"friend12\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " \"-2\" -> \"-1\":(\"friend14\", \"friend13\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))"
    val insertEdgeResp = session.execute(insertEdge)
    if (!insertEdgeResp.isSucceeded) {
      close()
      sys.exit(-1)
    }
  }

  def mockIntIdGraph(): Unit = {
    val session = pool.getSession("root", "nebula", true)

    val createSpace = "CREATE SPACE IF NOT EXISTS test_int(partition_num=10, vid_type=int64);" +
      "USE test_int;" + "CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16, col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double, col12 float, col13 time);" +
      "CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16, col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double, col12 float, col13 time);";
    val createResp = session.execute(createSpace)
    if (!createResp.isSucceeded) {
      close()
      sys.exit(-1)
    }

    val insertTag =
      "INSERT VERTEX person(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13) VALUES " +
        " 1:(\"person1\", \"person1\", 11, 200, 1000, 188888, date(\"2021-01-01\"), datetime(\"2021-01-01T12:00:00\"),timestamp(\"2021-01-01T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 2:(\"person2\", \"person2\", 12, 300, 2000, 288888, date(\"2021-01-02\"), datetime(\"2021-01-02T12:00:00\"),timestamp(\"2021-01-02T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " 3:(\"person3\", \"person3\", 13, 400, 3000, 388888, date(\"2021-01-03\"), datetime(\"2021-01-03T12:00:00\"),timestamp(\"2021-01-03T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " 4:(\"person4\", \"person4\", 14, 500, 4000, 488888, date(\"2021-01-04\"), datetime(\"2021-01-04T12:00:00\"),timestamp(\"2021-01-04T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 5:(\"person5\", \"person5\", 15, 600, 5000, 588888, date(\"2021-01-05\"), datetime(\"2021-01-05T12:00:00\"),timestamp(\"2021-01-05T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 6:(\"person6\", \"person6\", 16, 700, 6000, 688888, date(\"2021-01-06\"), datetime(\"2021-01-06T12:00:00\"),timestamp(\"2021-01-06T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " 7:(\"person7\", \"person7\", 17, 800, 7000, 788888, date(\"2021-01-07\"), datetime(\"2021-01-07T12:00:00\"),timestamp(\"2021-01-07T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 8:(\"person8\", \"person8\", 18, 900, 8000, 888888, date(\"2021-01-08\"), datetime(\"2021-01-08T12:00:00\"),timestamp(\"2021-01-08T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 9:(\"person9\", \"person9\", 19, 1000, 9000, 988888, date(\"2021-01-09\"), datetime(\"2021-01-09T12:00:00\"),timestamp(\"2021-01-09T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " 10:(\"person10\", \"person10\", 20, 1100, 10000, 1088888, date(\"2021-01-10\"), datetime(\"2021-01-10T12:00:00\"),timestamp(\"2021-01-10T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
        " 11:(\"person11\", \"person11\", 21, 1200, 11000, 1188888, date(\"2021-01-11\"), datetime(\"2021-01-11T12:00:00\"),timestamp(\"2021-01-11T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
        " 12:(\"person12\", \"person11\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))"
    val insertTagResp = session.execute(insertTag)
    if (!insertTagResp.isSucceeded) {
      close()
      sys.exit(-1)
    }

    val insertEdge = "INSERT EDGE friend(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13) VALUES " +
      " 1 -> 2:(\"friend1\", \"friend2\", 11, 200, 1000, 188888, date(\"2021-01-01\"), datetime(\"2021-01-01T12:00:00\"),timestamp(\"2021-01-01T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 2 -> 3:(\"friend2\", \"friend3\", 12, 300, 2000, 288888, date(\"2021-01-02\"), datetime(\"2021-01-02T12:00:00\"),timestamp(\"2021-01-02T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " 3 -> 4:(\"friend3\", \"friend4\", 13, 400, 3000, 388888, date(\"2021-01-03\"), datetime(\"2021-01-03T12:00:00\"),timestamp(\"2021-01-03T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " 4 -> 5:(\"friend4\", \"friend4\", 14, 500, 4000, 488888, date(\"2021-01-04\"), datetime(\"2021-01-04T12:00:00\"),timestamp(\"2021-01-04T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 5 -> 6:(\"friend5\", \"friend5\", 15, 600, 5000, 588888, date(\"2021-01-05\"), datetime(\"2021-01-05T12:00:00\"),timestamp(\"2021-01-05T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 6 -> 7:(\"friend6\", \"friend6\", 16, 700, 6000, 688888, date(\"2021-01-06\"), datetime(\"2021-01-06T12:00:00\"),timestamp(\"2021-01-06T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " 7 -> 8:(\"friend7\", \"friend7\", 17, 800, 7000, 788888, date(\"2021-01-07\"), datetime(\"2021-01-07T12:00:00\"),timestamp(\"2021-01-07T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 8 -> 9:(\"friend8\", \"friend8\", 18, 900, 8000, 888888, date(\"2021-01-08\"), datetime(\"2021-01-08T12:00:00\"),timestamp(\"2021-01-08T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 9 -> 10:(\"friend9\", \"friend9\", 19, 1000, 9000, 988888, date(\"2021-01-09\"), datetime(\"2021-01-09T12:00:00\"),timestamp(\"2021-01-09T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " 10 -> 11:(\"friend10\", \"friend10\", 20, 1100, 10000, 1088888, date(\"2021-01-10\"), datetime(\"2021-01-10T12:00:00\"),timestamp(\"2021-01-10T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))," +
      " 11 -> 12:(\"friend11\", \"friend11\", 21, 1200, 11000, 1188888, date(\"2021-01-11\"), datetime(\"2021-01-11T12:00:00\"),timestamp(\"2021-01-11T12:00:00\"), false, 1.0, 2.0, time(\"12:01:01\"))," +
      " 12 -> 1:(\"friend12\", \"friend11\", 22, 1300, 12000, 1288888, date(\"2021-01-12\"), datetime(\"2021-01-12T12:00:00\"),timestamp(\"2021-01-12T12:00:00\"), true, 1.0, 2.0, time(\"12:01:01\"))"
    val insertEdgeResp = session.execute(insertEdge)
    if (!insertEdgeResp.isSucceeded) {
      close()
      sys.exit(-1)
    }
  }

  def close(): Unit = {
    pool.close()
  }
}
