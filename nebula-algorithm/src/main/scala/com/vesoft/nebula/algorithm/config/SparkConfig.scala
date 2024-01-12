/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.config

import org.apache.spark.sql.SparkSession

case class SparkConfig(spark: SparkSession, partitionNum: Int)

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
    val spark = session.getOrCreate()
    validate(spark.version, "2.4.*")
    SparkConfig(spark, partitionNum.toInt)
  }

  def validate(sparkVersion: String, supportedVersions: String*): Unit = {
    if (sparkVersion != "UNKNOWN" && !supportedVersions.exists(sparkVersion.matches)) {
      throw new RuntimeException(
        s"""Your current spark version ${sparkVersion} is not supported by the current NebulaGraph Algorithm.
           | please visit https://github.com/vesoft-inc/nebula-algorithm#version-compatibility-matrix to know which version you need.
           | """.stripMargin)
    }
  }
}
