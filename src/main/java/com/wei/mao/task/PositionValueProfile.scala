package com.wei.mao.task

import com.google.protobuf.ByteString
import inco.common.log_process.{Common, LogProcess}
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.tencent.tdw.spark.toolkit.tdw.TDWProvider
import java.text.SimpleDateFormat

object PositionValueProfile {
  val dbName = "hlw_gdt"
  val META_GROUP = "tl"
  val user = "tdw_miraclemao"
  val password = "tdw_miraclemao"
  // 购物号，浏览器搜索，YF站点, 微视等站点排除
  val exclude_site = Set(70800426750600L, 60807476850604L, 40807403425677L,
                         20205430044969L, 90909430249968L, 20105445936502L, 80309495536503L)
  // 排除闪屏场景
  val exclude_scene = Set(8L)
  def getPositionValueProfile(sc: SparkContext, date: String)
    : RDD[(Long, Long, Double)] = {
    val tdw = new TDWProvider(sc, user=user, passwd=password, dbName=dbName)
    val dates = Common.getDatesList(date, 3)
    val date_list = dates.split(",").map(x => s"p_${x}").toSeq
    tdw.table("t_ocpa_middle_table_d", date_list)
    .map{ x =>
      //        val product_id = if (x(2) == "") 0L else x(2).toLong
//      val site_set = if (x(5) == "") 0L else x(5).toLong
      //        val advertiser_id = if (x(7) == "") 0L else x(7).toLong
      //        val advertiser_industry = if (x(9) == "") 0L else x(9).toLong
      //        val second_class_id = if (x(9) == "") 0L else x(9).toLong - 21474836480L
      //        val class_id = if (second_class_id == 0L) 0 else math.round(second_class_id / 100).toInt
      //        val adgroup_id = if (x(11) == "") 0L else x(11).toLong
      //        val smart_optimization_goal = if (x(13) == "") 0L else x(13).toLong
        val site_set = if(x(2) == "") 0L else x(2).toLong
        val site_id = if(x(6) == "") 0L else x(6).toLong
        val position_id = if(x(14) == "") 0L else x(14).toLong
        val req_cnt = if(x(56) == "") 0L else x(56).toLong
        val ret_cnt = if(x(57) == "") 0L else x(57).toLong
        val real_cost_micros = if(x(64) == "") 0.0 else x(64).toDouble / 1000000
        val scene = if(x(236) == "") 0L else x(236).toLong
      ((site_set, site_id, position_id, scene), (req_cnt, real_cost_micros, ret_cnt))
    }.filter(x => x._1._1 == 25 && !exclude_site.contains(x._1._2) && x._1._3 > 0L && !exclude_scene.contains(x._1._4))
    .map(x => (x._1._3, x._2))
    .reduceByKey{ (x, y) =>(x._1 + y._1, x._2 + y._2, x._3 + y._3)}
    .filter(x => x._2._1 > 0 && x._2._3 > 100)
    .map(x => (x._1, x._2._1, x._2._2 * 1000 / x._2._1)) // (position_id, (req_cnt, cpr)), cpr = cost * 1000 / req_cnt
  }

  def main(args: Array[String]) {
    val cmdArgs = Common.parseArgs(args)
    val sparkConf = new SparkConf().setAppName("getPositionValueProfile")
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val sc = new SparkContext(sparkConf)
    val day = cmdArgs.getOrElse("day", "20201201")
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/" + day)
    Common.deletePath(sc, outputPath)
    val pos_values = getPositionValueProfile(sc, day).collect()
    val total_req_cnt = pos_values.map(_._2).sum
    val pos_values_res = scala.collection.mutable.ListBuffer[(Int, Long, Long, Double, Int, Int)]()
    var cur_req_cnt = 0L
    pos_values.sortWith(_._3 < _._3).foreach{ row =>
        val pos_id = row._1
        val req_cnt = row._2
        val cpr = row._3
        cur_req_cnt  += req_cnt
        val cpr_quantile = math.ceil(cur_req_cnt * 100.0 / total_req_cnt).toInt
        pos_values_res.append((day.toInt, pos_id, req_cnt, cpr, cpr_quantile, 0))
    }
    sc.parallelize(pos_values_res.toList)
    .map(m =>  m.productIterator.mkString("\t"))
    .repartition(1).saveAsTextFile(outputPath)
 }
}
