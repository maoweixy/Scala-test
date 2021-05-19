package com.wei.mao.task

import com.gdt.dragon.DragonInputFormat
import com.gdt.proto.GDTPageview.Pageview
import inco.common.log_process.Common
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions.asScalaBuffer

object  SavePcxr{
  // 18个字段
  def getPcxr(sc: SparkContext, dataPath: String): RDD[String] = {
    sc.newAPIHadoopFile[NullWritable, BytesWritable, DragonInputFormat](dataPath)
      .map(_._2.copyBytes()).map(Pageview.parseFrom)
      .filter(pv => (pv.getPage.getSiteId == 70506421868294L || pv.getPage.getSiteId == 70702022115052L) && pv.getIdLocators.getQqOpenid.toStringUtf8.nonEmpty)
      .flatMap(pv =>
        pv.getPositionsList.flatMap(pos =>
          pos.getImpsList.filter(imp => imp.getAd.getSmartOptimizer.getOptimizationGoal > 0 && imp.getAd.getSmartOptimizer.getOptimizationGoal != 7)
            .map(imp => {
            val ad = imp.getAd

            val adCategorys = ad.getVerticalList.map(_.getId - 21474836480L).filter(_ >= 0)
            val second_class_id = if (adCategorys.nonEmpty) adCategorys.head else 0 // 二级类目
            val class_id = if (adCategorys.nonEmpty) math.round(adCategorys.head / 100).toInt else 0 // 一级类目

            Vector(pv.getPage.getSiteId, pv.getIdLocators.getQqOpenid.toStringUtf8, pos.getId, pos.getScene,

              ad.getQualityProductId, ad.getAdgroupId, ad.getAdvertiserId, ad.getProductId,
              ad.getSmartOptimizer.getOptimizationGoal, class_id, second_class_id,

              ad.getReranking.getPctr / 1000000.0F, ad.getPcvr.getAdjustedSmartPcvr,
              ad.getReranking.getTotalEcpm, ad.getReranking.getNextTotalEcpm,
              ad.getScoring.getCtr, ad.getScoring.getCvr,
              imp.getPingTime
            ).mkString("\t")
          }
          )
        )
      )
  }

  def main(args: Array[String]) {
    val cmdArgs = Common.parseArgs(args)
    val hour = cmdArgs.getOrElse("hour", "20210511")
    val inputPath = cmdArgs.getOrElse("inputPath", s"hdfs://ss-cdg-cold1-v2/data/MAPREDUCE/CDG/g_sng_gdt_gdt_log_process/threemonth/gdt/dragon/joined_exposure/" + hour  + "*/*")
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/pcxr/" + hour)


    val sparkConf = new SparkConf().setAppName("testdemo")
    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_miraclemao:tdw_miraclemao")
    val sc = new SparkContext(sparkConf)
    Common.deletePath(sc, outputPath)

    val DRAGON_FIELDNAMES = List("page.site_id", "id_locators.qq_openid", "positions.id", "positions.scene",

      "positions.imps.ad.quality_product_id", "positions.imps.ping_time", "positions.imps.ad.adgroup_id", "positions.imps.ad.advertiser_id", "positions.imps.ad.product_id",
      "positions.imps.ad.smart_optimizer.optimization_goal", "positions.imps.ad.vertical.id",

      "positions.imps.ad.reranking.pctr", "positions.imps.ad.pcvr.adjusted_smart_pcvr",
      "positions.imps.ad.reranking.total_ecpm", "positions.imps.ad.reranking.next_total_ecpm",
      "positions.imps.ad.scoring.ctr", "positions.imps.ad.scoring.cvr",
      "positions.imps.ping_time"
    ).mkString(",")

    sc.hadoopConfiguration.set("dragon.reader.fieldnames", DRAGON_FIELDNAMES)

    val data = getPcxr(sc, inputPath)

    data.repartition(20).saveAsTextFile(outputPath)
  }
}
