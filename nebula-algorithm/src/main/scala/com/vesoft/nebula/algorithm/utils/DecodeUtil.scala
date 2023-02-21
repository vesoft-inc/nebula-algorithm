/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.utils

import com.vesoft.nebula.algorithm.config.AlgoConstants.{ALGO_ID_COL, ENCODE_ID_COL, ORIGIN_ID_COL}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object DecodeUtil {

  def convertStringId2LongId(dataframe: DataFrame, hasWeight: Boolean): (DataFrame, DataFrame) = {
    val fieldNames         = dataframe.schema.fieldNames
    val (srcName, dstName) = (fieldNames(0), fieldNames(1))

    // get all vertex ids from edge dataframe
    val srcIdDF: DataFrame = dataframe.select(srcName).withColumnRenamed(srcName, ORIGIN_ID_COL)
    val dstIdDF: DataFrame = dataframe.select(dstName).withColumnRenamed(dstName, ORIGIN_ID_COL)
    val idDF               = srcIdDF.union(dstIdDF).distinct()
    idDF.show()

    // encode id to Long type using dense_rank, the encodeId has two columns: id, encodedId
    // then you need to save the encodeId to convert back for the algorithm's result.
    // val encodeId = idDF.withColumn("encodedId", dense_rank().over(Window.orderBy("id")))
    // using function monotonically_increasing_id(), please refer https://spark.apache.org/docs/3.0.2/api/java/org/apache/spark/sql/functions.html#monotonically_increasing_id--
    val encodeId = idDF.withColumn(ENCODE_ID_COL, monotonically_increasing_id())
    encodeId.cache()

    // convert the edge data's src and dst
    val srcJoinDF = dataframe
      .join(encodeId)
      .where(col(srcName) === col(ORIGIN_ID_COL))
      .drop(srcName)
      .drop(ORIGIN_ID_COL)
      .withColumnRenamed(ENCODE_ID_COL, srcName)
    srcJoinDF.cache()
    val dstJoinDF = srcJoinDF
      .join(encodeId)
      .where(col(dstName) === col(ORIGIN_ID_COL))
      .drop(dstName)
      .drop(ORIGIN_ID_COL)
      .withColumnRenamed(ENCODE_ID_COL, dstName)
    dstJoinDF.show()

    // make the first two columns of edge dataframe are src and dst id
    val encodeDataframe = if (hasWeight) {
      dstJoinDF.select(srcName, dstName, fieldNames(2))
    } else {
      dstJoinDF.select(srcName, dstName)
    }
    (encodeDataframe, encodeId)
  }

  def convertAlgoId2StringId(dataframe: DataFrame, encodeId: DataFrame): DataFrame = {
    encodeId
      .join(dataframe)
      .where(col(ENCODE_ID_COL) === col(ALGO_ID_COL))
      .drop(ENCODE_ID_COL)
      .drop(ALGO_ID_COL)
      .withColumnRenamed(ORIGIN_ID_COL, ALGO_ID_COL)
  }

  def convertAlgoResultId2StringId(dataframe: DataFrame,
                                   encodeId: DataFrame,
                                   algoProp: String): DataFrame = {
    val originIdResult = convertAlgoId2StringId(dataframe, encodeId)
    dataframe
      .join(encodeId)
      .where(col(algoProp) === col(ENCODE_ID_COL))
      .drop(ENCODE_ID_COL)
      .drop(algoProp)
      .withColumnRenamed(ORIGIN_ID_COL, algoProp)
    originIdResult
  }
}
