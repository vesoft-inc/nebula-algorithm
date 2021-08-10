/*
 * Copyright (c) 2021. vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.algorithm.lib

import com.vesoft.nebula.algorithm.config.{AlgoConstants, HanpConfig}
import com.vesoft.nebula.algorithm.utils.NebulaUtil
import org.apache.commons.math3.optim.MaxIter
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.prefs.Preferences

object HanpAlgo {
  val ALGORITHM: String = "Hanp"

  /**
   * run the Hanp algorithm for nebula graph
   */
  def apply(spark: SparkSession,
            dataset: Dataset[Row],
            hanpConfig: HanpConfig,
            hasWeight:Boolean,
            preferences:RDD[(VertexId,Double)]=null):DataFrame={
    val graph: Graph[None.type, Double] = NebulaUtil.loadInitGraph(dataset, hasWeight)
    val hanpResultRDD = execute(graph,hanpConfig.hopAttenuation,hanpConfig.maxIter,hanpConfig.preference,preferences)
    val schema = StructType(
      List(
        StructField(AlgoConstants.ALGO_ID_COL, LongType, nullable = false),
        StructField(AlgoConstants.HANP_RESULT_COL, LongType, nullable = true)
      ))
    val algoResult = spark.sqlContext.createDataFrame(hanpResultRDD, schema)
    algoResult
  }

  /**
   * execute Hanp algorithm
   */
  def execute(graph: Graph[None.type, Double],
              hopAttenuation:Double,
              maxIter: Int,
              preference:Double=1.0,
              preferences:RDD[(VertexId,Double)]=null):RDD[Row]={
    var hanpGraph: Graph[(VertexId, Double, Double), Double]=null
    if(preferences==null){
      hanpGraph=graph.mapVertices((vertexId,_)=>(vertexId,preference,1.0))
    }else{
      hanpGraph=graph.outerJoinVertices(preferences)((vertexId, _, vertexPreference) => {(vertexId,vertexPreference.getOrElse(preference),1.0)})
    }
    def sendMessage(e: EdgeTriplet[(VertexId,Double,Double), Double]): Iterator[(VertexId, Map[VertexId, (Double,Double)])] = {
      if(e.srcAttr._3>0 && e.dstAttr._3>0){
        Iterator(
          (e.dstId, Map(e.srcAttr._1 -> (e.srcAttr._3,e.srcAttr._2*e.srcAttr._3*e.attr))),
          (e.srcId, Map(e.dstAttr._1 -> (e.dstAttr._3,e.dstAttr._2*e.dstAttr._3*e.attr)))
        )
      }else if(e.srcAttr._3>0){
        Iterator((e.dstId, Map(e.srcAttr._1 -> (e.srcAttr._3,e.srcAttr._2*e.srcAttr._3*e.attr))))
      }else if(e.dstAttr._3>0){
        Iterator((e.srcId, Map(e.dstAttr._1 -> (e.dstAttr._3,e.dstAttr._2*e.dstAttr._3*e.attr))))
      }else{
        Iterator.empty
      }
    }
    def mergeMessage(count1: Map[VertexId, (Double,Double)], count2: Map[VertexId, (Double,Double)])
    : Map[VertexId, (Double,Double)] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, (0.0,0.0))
        val count2Val = count2.getOrElse(i, (0.0,0.0))
        i -> (Math.max(count1Val._1,count2Val._1),count1Val._2+count2Val._2)
      }(collection.breakOut)
    }
    def vertexProgram(vid: VertexId, attr: (VertexId,Double,Double), message: Map[VertexId, (Double,Double)]): (VertexId,Double,Double) = {
      if (message.isEmpty) {
        attr
      } else {
        val maxMessage=message.maxBy(_._2._2)
        (maxMessage._1,attr._2,maxMessage._2._1-hopAttenuation)
      }
    }
    val initialMessage = Map[VertexId, (Double,Double)]()
    val hanpResultGraph=hanpGraph.pregel(initialMessage,maxIter)(vertexProgram,sendMessage,mergeMessage)
    hanpResultGraph.vertices.map(vertex=>Row(vertex._1,vertex._2._1))
  }
}
