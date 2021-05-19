package com.wei.mao.task

import com.tencent.tdw.spark.toolkit.tdw.TDWProvider
import inco.common.log_process.Common
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RepeatedImpsPcxrAndCxr0 {
  val dbName = "hlw_gdt"
  val META_GROUP = "tl"
  val user = "tdw_miraclemao"
  val password = "tdw_miraclemao"

  //  (quality_product_id, (exposure_cnt, click_cnt, active_cnt, ctr, cvr))
  def getCxr(sc: SparkContext, date: String)
  : RDD[(Long, (Int, Int, Int, Float, Float))] = {
    val tdw = new TDWProvider(sc, user = user, passwd = password, dbName = dbName)
    val dates = Common.getDatesList(date, 3)
    val date_list = dates.split(",").map(x => s"p_${x}").toSeq
    tdw.table("t_ocpa_middle_table_d", date_list)
      .map { x =>
        val site_set = if (x(5) == "") 0L else x(5).toInt
        val active_num = if (x(27) == "") 0 else x(27).toInt
        val valid_exposure_cnt = if (x(37) == "") 0 else x(37).toInt
        val valid_click_cnt = if (x(38) == "") 0 else x(38).toInt
        val quality_product_id = if (x(81) == "") 0L else x(81).toLong

        (site_set, quality_product_id, valid_exposure_cnt, valid_click_cnt, active_num)
      }.filter(x => x._1 == 25)
      .map(x => (x._2, (x._3, x._4, x._5)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._2.toFloat / x._2._1, x._2._3.toFloat / x._2._2)))
  }

  def getPcxr(sc: SparkContext, inputPath: String): RDD[((Long, String), (Long, Float, Float, Int))] = {
    sc.textFile(inputPath).map(x => x.split("\t", -1))
      .map { line =>
        val site_id = line(0).toLong
        val open_id = line(1).toString
        val quality_product_id = line(4).toLong
        val pctr = line(11).toFloat
        val pcvr = line(12).toFloat
        val ping_time = line(17).toInt

        ((site_id, open_id), (quality_product_id, pctr, pcvr, ping_time))
      }
  }

  //site_id, open_id, quality_product_id, pctr, pcvr, ctr, cvr, exposure_cnt, click_cnt, active_cnt
  def joinPcxrAndCxr(data1: RDD[((Long, String), (Long, Float, Float, Int))], data2: RDD[(Long, (Int, Int, Int, Float, Float))]) = {
    //quality_product_id,(site_id, open_id pctr, pcvr)
    val pcxr: RDD[(Long, (Long, String, Float, Float))] = data1.groupByKey()
      //RDD[((site_id, open_id), Iterable[(quality_product_id, pctr, pcvr, ping_time)])]
      .flatMap(x => {
        val listBuffer = ListBuffer[(Long, Float, Float)]()
        val qPList = x._2.toList.sortBy(_._4).map(v => (v._1, v._2, v._3))
        val window_size = 10
        val repeated_num = 3

        qPList.grouped(window_size).foreach(x => {
          val window_size_map = mutable.Map[Long, List[(Long, Float, Float)]]().withDefaultValue(List())
          x.foreach(v => window_size_map(v._1) = window_size_map(v._1) :+ (v._1, v._2, v._3))
          window_size_map.foreach(x => {
            if (x._2.length >= repeated_num) listBuffer ++= x._2
          })
        })
        listBuffer.map(v => (v._1, (x._1._1, x._1._2, v._2, v._3)))
      })
    pcxr.join(data2).map(x => {
      //site_id, open_id, quality_product_id, pctr, pcvr,
      //ctr, cvr, exposure_cnt, click_cnt, active_cnt
      Vector(x._2._1._1, x._2._1._2, x._1, x._2._1._3, x._2._1._4,
        x._2._2._4, x._2._2._5, x._2._2._1, x._2._2._2, x._2._2._3
      ).mkString("\t")
    })
  }


  def main(args: Array[String]) {

    val cmdArgs = Common.parseArgs(args)
    val day = cmdArgs.getOrElse("hour", "20210511")

    val inputPath = cmdArgs.getOrElse("inputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr/" + day)
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr_and_cxr/" + day)

    val sparkConf = new SparkConf().setAppName("testdemo")
    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_miraclemao:tdw_miraclemao")
    val sc = new SparkContext(sparkConf)
    Common.deletePath(sc, outputPath)

    val data = joinPcxrAndCxr(getPcxr(sc, inputPath), getCxr(sc, day))

    data.repartition(20).saveAsTextFile(outputPath)
  }
}
