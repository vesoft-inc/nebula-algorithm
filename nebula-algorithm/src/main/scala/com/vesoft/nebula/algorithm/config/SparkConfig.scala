/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.config

import org.apache.spark.sql.SparkSession

case class SparkConfig(spark: SparkSession, partitionNum: String)

object SparkConfig {

  var spark: SparkSession = _

  var partitionNum: String = _

  def getSpark(configs: Configs, defaultAppName: String = "algorithm"): SparkConfig = {
    val sparkConfigs = configs.sparkConfig.map
    val session = SparkSession.builder
      .appName(defaultAppName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    for (key <- sparkConfigs.keySet) {
      session.config(key, sparkConfigs(key))
    }
    partitionNum = sparkConfigs.getOrElse("spark.app.partitionNum", "0")
    SparkConfig(session.getOrCreate(), partitionNum)
  }
}
