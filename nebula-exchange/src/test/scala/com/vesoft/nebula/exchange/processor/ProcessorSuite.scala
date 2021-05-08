/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package scala.com.vesoft.nebula.exchange.processor

import com.vesoft.nebula.exchange.processor.Processor
import com.vesoft.nebula.{Date, DateTime, NullType, Time, Value}
import com.vesoft.nebula.meta.PropertyType
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.junit.Test

class ProcessorSuite extends Processor {
  val values = List("Bob",
                    "fixedBob",
                    12,
                    200,
                    1000,
                    100000,
                    "2021-01-01",
                    "2021-01-01T12:00:00",
                    "12:00:00",
                    "2021-01-01T12:00:00",
                    true,
                    12.01,
                    22.12,
                    null)
  val schema: StructType = StructType(
    List(
      StructField("col1", StringType, nullable = true),
      StructField("col2", StringType, nullable = true),
      StructField("col3", ShortType, nullable = true),
      StructField("col4", ShortType, nullable = true),
      StructField("col5", IntegerType, nullable = true),
      StructField("col6", LongType, nullable = true),
      StructField("col7", StringType, nullable = true),
      StructField("col8", StringType, nullable = true),
      StructField("col9", StringType, nullable = true),
      StructField("col10", StringType, nullable = true),
      StructField("col11", BooleanType, nullable = true),
      StructField("col12", DoubleType, nullable = true),
      StructField("col13", DoubleType, nullable = true),
      StructField("col14", StringType, nullable = true)
    ))
  val row = new GenericRowWithSchema(values.toArray, schema)
  val map = Map(
    "col1"  -> PropertyType.STRING,
    "col2"  -> PropertyType.FIXED_STRING,
    "col3"  -> PropertyType.INT8,
    "col4"  -> PropertyType.INT16,
    "col5"  -> PropertyType.INT32,
    "col6"  -> PropertyType.INT64,
    "col7"  -> PropertyType.DATE,
    "col8"  -> PropertyType.DATETIME,
    "col9"  -> PropertyType.TIME,
    "col10" -> PropertyType.TIMESTAMP,
    "col11" -> PropertyType.BOOL,
    "col12" -> PropertyType.DOUBLE,
    "col13" -> PropertyType.FLOAT,
    "col14" -> PropertyType.STRING
  )

  @Test
  def extraValueForClientSuite(): Unit = {
    assert(extraValueForClient(row, "col1", map).toString.equals("\"Bob\""))
    assert(extraValueForClient(row, "col2", map).toString.equals("\"fixedBob\""))
    assert(extraValueForClient(row, "col3", map).toString.toInt == 12)
    assert(extraValueForClient(row, "col4", map).toString.toInt == 200)
    assert(extraValueForClient(row, "col5", map).toString.toInt == 1000)
    assert(extraValueForClient(row, "col6", map).toString.toLong == 100000)
    assert(extraValueForClient(row, "col7", map).toString.equals("date(\"2021-01-01\")"))
    assert(
      extraValueForClient(row, "col8", map).toString.equals("datetime(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClient(row, "col9", map).toString.equals("time(\"12:00:00\")"))
    assert(
      extraValueForClient(row, "col10", map).toString.equals("timestamp(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClient(row, "col11", map).toString.toBoolean)
    assert(extraValueForClient(row, "col12", map).toString.toDouble > 12.00)
    assert(extraValueForClient(row, "col13", map).toString.toDouble > 22.10)
    assert(extraValueForClient(row, "col14", map) == null)
  }

  @Test
  def extraValueForSSTSuite(): Unit = {
    assert(extraValueForSST(row, "col1", map).toString.equals("Bob"))
    assert(extraValueForSST(row, "col2", map).toString.equals("fixedBob"))
    assert(extraValueForSST(row, "col3", map).toString.toInt == 12)
    assert(extraValueForSST(row, "col4", map).toString.toInt == 200)
    assert(extraValueForSST(row, "col5", map).toString.toInt == 1000)
    assert(extraValueForSST(row, "col6", map).toString.toLong == 100000)
    val date = new Date(2021, 1, 1)
    assert(extraValueForSST(row, "col7", map).equals(date))
    val datetime = new DateTime(2021, 1, 1, 12, 0, 0, 0)
    assert(extraValueForSST(row, "col8", map).equals(datetime))

    val time = new Time(12, 0, 0, 0)
    assert(extraValueForSST(row, "col9", map).equals(time))

    try {
      extraValueForSST(row, "col10", map).toString
    } catch {
      case e: Exception => assert(true)
    }

    assert(extraValueForSST(row, "col11", map).toString.toBoolean)
    assert(extraValueForSST(row, "col12", map).toString.toDouble > 12.0)
    assert(extraValueForSST(row, "col13", map).toString.toFloat > 22.10)

    val nullValue = new Value()
    nullValue.setNVal(NullType.__NULL__)
    assert(extraValueForSST(row, "col14", map).equals(nullValue))
  }

  /**
    * process dataframe to vertices or edges
    */
  override def process(): Unit = ???

}
