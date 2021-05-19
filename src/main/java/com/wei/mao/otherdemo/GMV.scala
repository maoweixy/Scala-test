package com.wei.mao.otherdemo

/*
  * 需求：分析GMV数据,包含实验id
  * 每个广告对应的转化数，消耗
  *
  * */

import com.gdt.proto.GDTPageview.Pageview
import com.google.protobuf.ByteString
import inco.common.log_process.{Common, LogProcess}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable

object GMV {
  //站点、广告位、实验ID、广告、优化目标、第二优化目标、目标出价、partition_time、process_time、转化数、消耗、gmv、曝光、点击、gmv_bias
  def getExpoData(sc: SparkContext, date: String, exp_ids: String): RDD[(String)] = {
    val ids = exp_ids.split(",").map { m =>
      m.toInt
    }
    val one_million = 1000000.0
    val one_hundred = 100.0
    LogProcess.getExposureLog(sc, date).flatMap { v =>
      val pv = v._1
      val id = pv.getId
      val process_time = LogProcess.convertToDate(pv.getProcessTime)
      val partition_time = pv.getPartitionDatetime.toString.substring(0, 8)
      val site_id = pv.getPage.getSiteId.toString
      pv.getPositionsList.flatMap { pos =>
        val position_id: Long = pos.getId
        pos.getImpsList.flatMap { imp =>
          val adgroup_id = imp.getAd.getAdgroupId
          val goal = imp.getAd.getSmartOptimizer.getOptimizationGoal
          val second_goal = imp.getAd.getSmartOptimizer.getSecondGoal
          val target_cpa = imp.getAd.getSmartOptimizer.getTargetCpa
          val gmv_bias = if (imp.getAd.getSmartOptimizer.hasGmvBias) imp.getAd.getSmartOptimizer.getGmvBias else 1.0
          val cpm_cost: Double = if (imp.hasBillCpm && imp.getBillCpm.hasRealCostMicros) imp.getBillCpm.getRealCostMicros.toDouble/one_million else 0.0
          pos.getExpIdList.map { m =>
            val exp_id: Int = m
            if (ids.contains(exp_id)) {
              ((site_id, position_id, exp_id, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                (0, cpm_cost, 0, 1, 0, gmv_bias))
            } else {
              ((site_id, position_id, 0, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                (0, 0.0, 0, 0, 0, 0.0))
            }
          }
        }
      }
    }.filter(f => f._1._3 != 0)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
      .map { m =>
        Vector(
          m._1._1, // 站点
          m._1._2, // 广告位
          m._1._3, // 实验id
          m._1._4, // 广告
          m._1._5, // 优化目标
          m._1._6, // 第二优化目标
          m._1._7, // 目标出价
          m._1._8, // partition_time
          m._1._9, // process_time
          m._2._1, // 转化数
          m._2._2, // 消耗
          m._2._3, // gmv
          m._2._4, // 曝光
          m._2._5, // 点击
          m._2._6  // gmv_bias因子
        ).mkString("\t")
      }
  }

  //站点、广告位、实验ID、广告、优化目标、目标出价、转化数、消耗
  def getClickData(sc: SparkContext, date: String, exp_ids: String): RDD[(String)] = {
    val ids = exp_ids.split(",").map { m =>
      m.toInt
    }
    val one_million = 1000000.0
    val one_hundred = 100.0
    LogProcess.getClickLog(sc, date).flatMap { v =>
      val pv = v._1
      val id = pv.getId
      val site_id = pv.getPage.getSiteId.toString
      val process_time = LogProcess.convertToDate(pv.getProcessTime)
      val partition_time = pv.getPartitionDatetime.toString.substring(0, 8)
      pv.getPositionsList.flatMap { pos =>
        val position_id: Long = pos.getId
        pos.getImpsList.flatMap { imp =>
          val adgroup_id = imp.getAd.getAdgroupId
          val goal = imp.getAd.getSmartOptimizer.getOptimizationGoal
          val second_goal = imp.getAd.getSmartOptimizer.getSecondGoal
          val target_cpa = imp.getAd.getSmartOptimizer.getTargetCpa
          imp.getClicksList.flatMap { clk =>
            val cpc_cost: Double = if (clk.hasBillCpc && clk.getBillCpc.hasRealCostMicros) clk.getBillCpc.getRealCostMicros.toDouble/one_million else 0.0
            pos.getExpIdList.map { m =>
              val exp_id: Int = m
              if (ids.contains(exp_id)) {
                ((site_id, position_id, exp_id, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                  (0, cpc_cost, 0, 0, 1, 0.0))
              } else {
                ((site_id, position_id, 0, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                  (0, 0.0, 0, 0, 0, 0.0))
              }
            }
          }
        }
      }
    }.filter(f => f._1._3 != 0)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
      .map { m =>
        Vector(
          m._1._1, // 站点
          m._1._2, // 广告位
          m._1._3, // 实验id
          m._1._4, // 广告
          m._1._5, // 优化目标
          m._1._6, // 第二优化目标
          m._1._7, // 目标出价
          m._1._8, // partition_time
          m._1._9, // process_time
          m._2._1, // 转化数
          m._2._2, // 消耗++
          m._2._3, // gmv
          m._2._4, // 曝光
          m._2._5,  // 点击
          m._2._6  // gmv_bias因子
        ).mkString("\t")
      }
  }

  def is_jd_platform_trade(product_type:Int,
                           action_type:Int,
                           trade_type:Int): Boolean ={
    if (product_type == 25 && action_type == 204) {
      if (trade_type != 1 && trade_type != 2) {
        return true
      }
    }
    return false
  }

  //站点、广告位、实验ID、广告、优化目标、目标出价、转化数、消耗
  def getTraceData(sc: SparkContext, date: String, exp_ids: String): RDD[(String)] = {
    val ids = exp_ids.split(",").map { m =>
      m.toInt
    }
    val one_million = 1000000.0
    val one_hundred = 100.0
    LogProcess.getTraceLog(sc, date).flatMap { v =>
      val pv = v._1
      val site_id = pv.getPage.getSiteId.toString
      val process_time = LogProcess.convertToDate(pv.getProcessTime)
      val partition_time = pv.getPartitionDatetime.toString.substring(0, 8)
      pv.getPositionsList.flatMap { pos =>
        val position_id: Long = pos.getId
        pos.getImpsList.flatMap { imp =>
          val adgroup_id = imp.getAd.getAdgroupId
          val goal = imp.getAd.getSmartOptimizer.getOptimizationGoal
          val second_goal = imp.getAd.getSmartOptimizer.getSecondGoal
          val target_cpa = imp.getAd.getSmartOptimizer.getTargetCpa
          val product_type = imp.getAd.getProductType.getNumber.toInt
          imp.getClicksList.flatMap { clk =>
            clk.getTracesList.flatMap { trace =>
              val action_type = trace.getActionTrackInfo.getType
              val trade_type = trace.getTradeType.getNumber
              val achieved_optimization_goal = trace.getAchivedOptimizationGoalList.toList
              // println("goal:" + goal + " achieved_optimization_goal:" + achieved_optimization_goal.mkString("\t"))
              var cnt: Int = 0
              if (goal > 0 && !is_jd_platform_trade(product_type, action_type, trade_type)) {
                if (action_type == goal) {
                  cnt = 1
                }
                // 下边这种取法有问题，数据不对
//                if(achieved_optimization_goal.contains(goal)) {
//                  cnt = 1
//                }
              }
              val gmv = cnt * target_cpa / one_hundred
              pos.getExpIdList.map { m =>
                val exp_id: Int = m
                if (ids.contains(exp_id)) {
                  ((site_id, position_id, exp_id, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                    (cnt, 0, gmv, 0, 0, 0.0))
                } else {
                  ((site_id, position_id, 0, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time),
                    (0, 0, gmv, 0, 0, 0.0))
                }
              }
            }
          }
        }
      }
    }.filter(f => f._1._3 != 0)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
      .map { m =>
        Vector(
          m._1._1, // 站点
          m._1._2, // 广告位
          m._1._3, // 实验id
          m._1._4, // 广告
          m._1._5, // 优化目标
          m._1._6, // 第二优化目标
          m._1._7, // 目标出价
          m._1._8, // partition_time
          m._1._9, // process_time
          m._2._1, // 转化数
          m._2._2, // 消耗
          m._2._3, // gmv
          m._2._4, // 曝光
          m._2._5,  // 点击
          m._2._6  // gmv_bias因子
        ).mkString("\t")
      }
  }

  // 站点、广告位、实验ID、广告、优化目标、目标出价、转化数、消耗
  def getMergeData(expoData: RDD[(String)], clickData: RDD[(String)], traceData: RDD[(String)]): RDD[(String)] = {
    // site_id, position_id, exp_id, adgroup_id, goal, second_goal, target_cpa, partition_time, process_time
    val data1 = expoData.map { m =>
      val ls = m.split("\t")
      val site_id: String = ls(0).toString
      val position_id: Long = ls(1).toLong
      val exp_id: Int = ls(2).toInt
      val ad_id: Long = ls(3).toLong
      val goal: Int = ls(4).toInt
      val second_goal: Int = ls(5).toInt
      val target_cpa: Int = ls(6).toInt
      val partition_time: String = ls(7).toString
      val process_time: String = ls(8).toString
      val trace_cnt: Int = ls(9).toInt
      val cost: Double = ls(10).toDouble
      val gmv: Double = ls(11).toDouble
      val imp_num: Int = ls(12).toInt
      val click_num: Int = ls(13).toInt
      val gmv_bias: Double = ls(14).toDouble
      ((site_id, position_id, exp_id, ad_id, goal, second_goal, target_cpa, partition_time, process_time),
        (trace_cnt, cost, gmv, imp_num, click_num, gmv_bias))
    }
    val data2 = clickData.map { m =>
      val ls = m.split("\t")
      val site_id: String = ls(0).toString
      val position_id: Long = ls(1).toLong
      val exp_id: Int = ls(2).toInt
      val ad_id: Long = ls(3).toLong
      val goal: Int = ls(4).toInt
      val second_goal: Int = ls(5).toInt
      val target_cpa: Int = ls(6).toInt
      val partition_time: String = ls(7).toString
      val process_time: String = ls(8).toString
      val trace_cnt: Int = ls(9).toInt
      val cost: Double = ls(10).toDouble
      val gmv: Double = ls(11).toDouble
      val imp_num: Int = ls(12).toInt
      val click_num: Int = ls(13).toInt
      val gmv_bias: Double = ls(14).toDouble
      ((site_id, position_id, exp_id, ad_id, goal, second_goal, target_cpa, partition_time, process_time),
        (trace_cnt, cost, gmv, imp_num, click_num, gmv_bias))
    }
    val data3 = traceData.map { m =>
      val ls = m.split("\t")
      val site_id: String = ls(0).toString
      val position_id: Long = ls(1).toLong
      val exp_id: Int = ls(2).toInt
      val ad_id: Long = ls(3).toLong
      val goal: Int = ls(4).toInt
      val second_goal: Int = ls(5).toInt
      val target_cpa: Int = ls(6).toInt
      val partition_time: String = ls(7).toString
      val process_time: String = ls(8).toString
      val trace_cnt: Int = ls(9).toInt
      val cost: Double = ls(10).toDouble
      val gmv: Double = ls(11).toDouble
      val imp_num: Int = ls(12).toInt
      val click_num: Int = ls(13).toInt
      val gmv_bias: Double = ls(14).toDouble
      ((site_id, position_id, exp_id, ad_id, goal, second_goal, target_cpa, partition_time, process_time),
        (trace_cnt, cost, gmv, imp_num, click_num, gmv_bias))
    }

    data1.union(data2).union(data3).reduceByKey((x, y) =>
      (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
      .map { m =>
        Vector(m._1._1, m._1._2, m._1._3, m._1._4, m._1._5, m._1._6, m._1._7, m._1._8, m._1._9,
          m._2._1, m._2._2, m._2._3, m._2._4, m._2._5, m._2._6).mkString("\t")
      }
  }

  def main(args: Array[String]) {
    val cmdArgs = Common.parseArgs(args)
    val hour = cmdArgs.getOrElse("hour", "2020032708")
    val exp_ids = cmdArgs.getOrElse("exp_ids", "82734,82735,82736")

    // debug config
//    System.setProperty("hadoop.home.dir", "E:\\workspace\\libs\\hadoop-common-2.2.0")
//    val test_data_dir = "file:/E:/workspace/IdeaProjects/midData/test_data/"
//    val outputPath = test_data_dir + "result_gmv/"
//    val sparkConf = new SparkConf().setAppName("GMV").setMaster("local")

    val sparkConf = new SparkConf().setAppName("GMV")
    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_hongmeihou:tdw_hongmeihou")

    val sparkContext = new SparkContext(sparkConf)
    val outputPath = cmdArgs.getOrElse("outputPath",
      s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/onemonth/zerokzou/cost_gmv_expr_analysis/raw_data/$hour/")
    Common.deletePath(sparkContext, outputPath)

    val expoData = getExpoData(sparkContext, hour, exp_ids)
    val clickData = getClickData(sparkContext, hour, exp_ids)
    val traceData = getTraceData(sparkContext, hour, exp_ids)
    println("exposureRdd.count:" + expoData.count())
    expoData.repartition(numPartitions = 100).saveAsTextFile(outputPath + "expo_data")
    println("clickRdd.count:" + clickData.count())
    clickData.repartition(numPartitions = 100).saveAsTextFile(outputPath + "click_data")
    traceData.repartition(numPartitions = 100).saveAsTextFile(outputPath + "trace_data")
    val mergeData = getMergeData(expoData, clickData, traceData)
    println("mergeDataRdd.count:" + mergeData.count())
    mergeData.repartition(100).saveAsTextFile(outputPath + "merge_data")
  }
}
