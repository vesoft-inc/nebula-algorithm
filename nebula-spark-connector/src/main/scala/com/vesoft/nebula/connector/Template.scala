/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

object NebulaTemplate {

  private[connector] val BATCH_INSERT_TEMPLATE               = "INSERT %s `%s`(%s) VALUES %s"
  private[connector] val VERTEX_VALUE_TEMPLATE               = "%s: (%s)"
  private[connector] val VERTEX_VALUE_TEMPLATE_WITH_POLICY   = "%s(\"%s\"): (%s)"
  private[connector] val ENDPOINT_TEMPLATE                   = "%s(\"%s\")"
  private[connector] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)"
  private[connector] val EDGE_VALUE_TEMPLATE                 = "%s->%s@%d: (%s)"
  private[connector] val USE_TEMPLATE                        = "USE %s"

  private[connector] val UPDATE_VERTEX_TEMPLATE = "UPDATE %s ON `%s` %s SET %s"
  private[connector] val UPDATE_EDGE_TEMPLATE   = "UPDATE %s ON `%s` %s->%s@%d SET %s"
  private[connector] val UPDATE_VALUE_TEMPLATE  = "`%s`=%s"

  private[connector] val DELETE_VERTEX_TEMPLATE = "DELETE VERTEX %s"
  private[connector] val DELETE_EDGE_TEMPLATE   = "DELETE EDGE `%s` %s"
  private[connector] val EDGE_ENDPOINT_TEMPLATE = "%s->%s@%d"
}
