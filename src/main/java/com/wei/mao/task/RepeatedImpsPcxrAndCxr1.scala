package com.wei.mao.task

import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import inco.common.log_process.Common
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RepeatedImpsPcxrAndCxr1 {
  val dbName = "hlw_gdt"
  val META_GROUP = "tl"
  val user = "tdw_miraclemao"
  val password = "tdw_miraclemao"

  //  (quality_product_id, (exposure_cnt, click_cnt, active_cnt, ctr, cvr))
  def getCxr(ss: SparkSession, date: String, filedName: String)
  : RDD[(String, (Int, Int, Int, Float, Float))] = {
    val tdw = new TDWSQLProvider(ss, user = user, passwd = password, dbName = dbName)
    val dates = Common.getDatesList(date, 3)
    val date_list = dates.split(",").map(x => s"p_${x}").toSeq
    tdw.table("t_ocpa_middle_table_d", date_list)
      .filter(s"process_date >= ${date} and process_date <= ${date} and site_set in (25)")
      .groupBy(s"${filedName}")
      .agg(sum("valid_exposure_cnt") as "exposure_cnt",
        sum("valid_click_cnt") as "click_cnt",
        sum("active_num") as "active_num")
      .select(s"${filedName}", "exposure_cnt", "click_cnt", "active_num").rdd
      .map(
        x => (x(0).toString, (x(1).toString.toInt, x(2).toString.toInt, x(3).toString.toInt,
          x(2).toString.toFloat / x(1).toString.toInt, x(3).toString.toFloat / x(2).toString.toInt))
      )
  }

  //((site_id, open_id), (quality_product_id, pctr, pcvr, ping_time))
  def getPcxr(sc: SparkContext, inputPath: String): RDD[((Long, String), (String, Float, Float, Int))] = {
    sc.textFile(inputPath).map(x => x.split("\t", -1))
      .map { line =>
        val site_id = line(0).toLong
        val open_id = line(1).toString
        val quality_product_id = line(4).toString
        val pctr = line(11).toFloat
        val pcvr = line(12).toFloat
        val ping_time = line(17).toInt

        ((site_id, open_id), (quality_product_id, pctr, pcvr, ping_time))
      }
  }

  //site_id, open_id, quality_product_id, pctr, pcvr, ctr, cvr, exposure_cnt, click_cnt, active_cnt
  def joinPcxrAndCxr(pcxr: RDD[((Long, String), (String, Float, Float, Int))], cxr: RDD[(String, (Int, Int, Int, Float, Float))]): RDD[String] = {
    //quality_product_id,(site_id, open_id pctr, pcvr)
    val RepeatedImpsPcxr: RDD[(String, (Long, String, Float, Float, Int))] = pcxr.groupByKey()
      //RDD[((site_id, open_id), Iterable[(quality_product_id, pctr, pcvr, ping_time)])]
      .flatMap(x => {
        val listBuffer = ListBuffer[(String, Float, Float, Int)]()
        val qPList = x._2.toList.map(v => (v._1, v._2, v._3, v._4))
        val window_size = 10
        val repeated_num = 3

        qPList.grouped(window_size).foreach(x => {
          x.groupBy(_._1).filter(x => x._2.length >= repeated_num)
            .flatMap(x => x._2.sortBy(_._4)).zipWithIndex.foreach(x => {
            listBuffer.append((x._1._1, x._1._2, x._1._3, x._2))
          })
        })

        listBuffer.map(v => (v._1, (x._1._1, x._1._2, v._2, v._3, v._4)))
      })

    RepeatedImpsPcxr.join(cxr).map(x => {
      //site_id, open_id, quality_product_id, pctr, pcvr, 出现顺序,
      //ctr, cvr, exposure_cnt, click_cnt, active_cnt
      Vector(x._2._1._1, x._2._1._2, x._1, x._2._1._3, x._2._1._4, x._2._1._5,
        x._2._2._4, x._2._2._5, x._2._2._1, x._2._2._2, x._2._2._3
      ).mkString("\t")
    })
  }


  def main(args: Array[String]) {

    val cmdArgs = Common.parseArgs(args)
    val day = cmdArgs.getOrElse("hour", "20210511")
    val filedName = cmdArgs.getOrElse("filedName", "quality_product_id")

    val inputPath = cmdArgs.getOrElse("inputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr/" + day)
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr_and_cxr/" + day)

    val sparkConf = new SparkConf().setAppName("testdemo")
    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_miraclemao:tdw_miraclemao")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    Common.deletePath(sc, outputPath)

    val data = joinPcxrAndCxr(getPcxr(sc, inputPath), getCxr(sparkSession, day, filedName))

    data.repartition(20).saveAsTextFile(outputPath)
  }
}
