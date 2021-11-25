/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.algorithm.config

object NebulaConfig {

  def getReadNebula(configs: Configs): NebulaReadConfigEntry = {
    val nebulaConfigs = configs.nebulaConfig
    nebulaConfigs.readConfigEntry
  }

  def getWriteNebula(configs: Configs): NebulaWriteConfigEntry = {
    val nebulaConfigs = configs.nebulaConfig
    nebulaConfigs.writeConfigEntry
  }
}
