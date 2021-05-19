package com.wei.mao.task


import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import inco.common.log_process.Common
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{avg, col, sum, abs}
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable.ListBuffer

object RepeatedImpsPcxrAndCxr {
  val sparkConf = new SparkConf().setAppName("testdemo")
  sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_miraclemao:tdw_miraclemao")
  val sc = new SparkContext(sparkConf)
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  val dbName = "hlw_gdt"
  val META_GROUP = "tl"
  val user = "tdw_miraclemao"
  val password = "tdw_miraclemao"

  TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
  val df = new SimpleDateFormat("yyyyMMdd")

  // 按偏移天数计算日期
  def getDayWithOffset(currentDay: String, dayOffset: Int): String = {
    val calendar = Calendar.getInstance()
    calendar.setTime(df.parse(currentDay))
    calendar.add(Calendar.DATE, dayOffset)
    df.format(calendar.getTime)
  }

  def getDatesList(currentDay: String, dayOffset: Int): String = {
    var daysList = List[String]()
    for (offset <- 0 to dayOffset - 1) {
      daysList = daysList :+ getDayWithOffset(currentDay, offset)
    }
    daysList.mkString(",")
  }

  //  (quality_product_id, (exposure_cnt, click_cnt, active_cnt, ctr, cvr))
  def getCxr(ss: SparkSession, date: String, filedName: String): DataFrame = {
    val tdw = new TDWSQLProvider(ss, user = user, passwd = password, dbName = dbName)
    val dates = getDatesList(date, 3)
    val date_list = dates.split(",").map(x => s"p_${x}").toSeq
    tdw.table("t_ocpa_middle_table_d", date_list)
      .filter(s"process_date >= ${date} and process_date <= ${date} and " +
        s"site_set in (25) and smart_optimization_goal is not null and smart_optimization_goal > 0 and smart_optimization_goal != 7")
      .select(col(filedName), col("valid_exposure_cnt").cast(LongType), col("valid_click_cnt").cast(LongType), col("active_num").cast(LongType))
      .groupBy(filedName)
      .agg(sum("valid_exposure_cnt") as "exposure_cnt",
        sum("valid_click_cnt") as "click_cnt",
        sum("active_num") as "active_cnt")
      .toDF("filed_name", "exposure_cnt", "click_cnt", "active_cnt")
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
  def joinPcxrAndCxr(pcxr: RDD[((Long, String), (String, Float, Float, Int))], cxr: DataFrame, windowSize: Int, repeatedNum: Int) = {
    //filedName,(pctr, pcvr, 出现次数)
    val RepeatedImpsPcxr = pcxr.groupByKey()
      //RDD[((site_id, open_id), Iterable[(quality_product_id, pctr, pcvr, ping_time)])]
      .flatMap(x => {
        val listBuffer = ListBuffer[(String, Float, Float, Int)]()
        //quality_product_id, pctr, pcvr, ping_time
        val qPList = x._2.toList.map(v => (v._1, v._2, v._3, v._4))

        qPList.grouped(windowSize).foreach(x => {
          x.groupBy(_._1).filter(x => x._2.length >= repeatedNum)
            .flatMap(x => {
              x._2.sortBy(_._4).zipWithIndex.map(v => (v._1._1, v._1._2, v._1._3, v._2))
            }).foreach(x => {
            listBuffer.append((x._1, x._2, x._3, x._4))
          })
        })

        listBuffer.map(v => (v._1, v._2, v._3, v._4))
      }).toDF("filed_name", "pctr", "pcvr", "order")


    RepeatedImpsPcxr.groupBy("filed_name", "order").agg(avg("pctr").as("pctr"),
      avg("pcvr").as("pcvr"))
      .join(cxr, "filed_name")
      .withColumn("ctr_bias", abs(col("pctr") / (col("click_cnt") / col("exposure_cnt")) - 1))
      .withColumn("cvr_bias", abs(col("pcvr") / (col("active_cnt") / col("click_cnt")) - 1))
      .groupBy("order").agg(avg("ctr_bias").as("ctr_bias"),
      avg("cvr_bias").as("cvr_bias"),
      sum("exposure_cnt").as("exposure_cnt"),
      sum("click_cnt").as("click_cnt"),
      sum("active_cnt").as("active_cnt"))
      //      .select("order", "ctr_bias", "cvr_bias")
      .select("order", "ctr_bias", "cvr_bias", "exposure_cnt", "click_cnt", "active_cnt")
  }


  def main(args: Array[String]) {

    val cmdArgs = Common.parseArgs(args)
    val day = cmdArgs.getOrElse("hour", "20210511")
    val windowSize = cmdArgs.getOrElse("window_size", "10").toInt
    val repeatedNum = cmdArgs.getOrElse("repeated_num", "3").toInt
    val filedName = cmdArgs.getOrElse("filed_name", "quality_product_id")

    val inputPath = cmdArgs.getOrElse("inputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr/" + day)
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr_and_cxr/" + day)
    val cxrOutputPath = cmdArgs.getOrElse("cxrOutputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/cxr/" + day)

    Common.deletePath(sc, outputPath)
    Common.deletePath(sc, cxrOutputPath)

    val cxr = getCxr(sparkSession, day, filedName)
    val data = joinPcxrAndCxr(getPcxr(sc, inputPath), cxr, windowSize, repeatedNum).orderBy("order")

    cxr.repartition(1).write.option("header", true).csv(cxrOutputPath)
    data.repartition(1).write.option("header", true).csv(outputPath)
  }
}
