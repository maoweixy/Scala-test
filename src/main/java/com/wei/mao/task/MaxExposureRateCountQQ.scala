package com.wei.mao.task

import com.gdt.dragon.DragonInputFormat
import com.gdt.proto.GDTPageview.Pageview
import inco.common.log_process.Common
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions.`deprecated asScalaBuffer`
import scala.collection.mutable

object MaxExposureRateCountQQ {
  def getAdClkConv(sc: SparkContext, hour: String): RDD[(Long, (Int, Int))] = {
    val data_path = "hdfs://ss-cdg-3-v2/stage/outface/sng/g_sng_gdt_isd_gdt/ad_statistics_data/" + hour
    sc.textFile(data_path).map(x => x.split("\t", -1))
      .map { line =>
        val adgroup = line(0).toLong
        val clk_cnt = line(1).toInt
        val conv_cnt = line(2).toInt
        (adgroup, (clk_cnt, conv_cnt))
      }
  }

  def getTrackCtrCvrConsitency(sc: SparkContext, dataPath: String): RDD[(Long, (Int, String, Int))] = {
    Common.getPageviewData(sc, dataPath)
      .filter(pv => pv.getPage.getSiteSet == 25)
      .flatMap { pv =>
        pv.getPositionsList
          .filter(pos => pos.getMixerTrack.getOmGetAdType != 1 && pos.getMixerTrack.getAdsList.count(ad => ad.getIsInReranking) > 0 && pos.getMixerTrack.getExpIdCount > 0)
          .flatMap { pos =>
            val adLiteCtrCvr = mutable.Map[Int, (Int, Int)]()
            pos.getRetrievalTracksList.foreach { track =>
              track.getAdsList.foreach { ad =>
                if (ad.hasAindex && ad.getScoringFactor.hasCtr && ad.getScoringFactor.hasCvr) {
                  val lite_ctr = ad.getScoringFactor.getCtr
                  val lite_cvr = ad.getScoringFactor.getCvr
                  adLiteCtrCvr += (ad.getAindex -> (lite_ctr, lite_cvr))
                }
              }
            }
            println("hello1")
            pos.getMixerTrack.getAdsList.filter(ad => ad.getRerankingInfo.hasPctr && ad.getRerankingInfo.hasAdjustedSmartPcvr)
              .map { ad =>
                val adgroup = ad.getAdgroupId
                val productId = ad.getProductId
                val advertiserId = ad.getRerankingInfo.getAdvertiserId

                val pctr = ad.getRerankingInfo.getPctr / 1000000.0
                val pcvr = ad.getRerankingInfo.getAdjustedSmartPcvr
                val values = adLiteCtrCvr.getOrElse(ad.getAindex, (0, 0))
                val lite_ctr = values._1 / 1000000.0
                val lite_cvr = values._2 / 1000000.0
                if (pctr > 0.0 && pcvr > 0.0 && lite_ctr > 0.0 && lite_cvr > 0.0) {
                  (adgroup, (((lite_ctr * lite_cvr * 100) / (pctr * pcvr)).toInt, productId, advertiserId))
                } else {
                  (adgroup, (0, "", 0))
                }
              }
          }
      }.filter(_._2._1 > 0)
  }


  //统计手Q、浏览器的重复曝光率
  // site_id, qPList_length(n), repeatedNum(p), count(qq)
  def getMaxExposureRateCountQQ(sc: SparkContext, dataPath: String): RDD[(Long, Int, Int, Int)] = {
    sc.newAPIHadoopFile[NullWritable, BytesWritable, DragonInputFormat](dataPath)
      .map(_._2.copyBytes()).map(Pageview.parseFrom)
      .filter(pv => (pv.getPage.getSiteId == 70506421868294L || pv.getPage.getSiteId == 70702022115052L) && pv.getIdLocators.getQqOpenid.toStringUtf8.nonEmpty)
      .flatMap(pv =>
        pv.getPositionsList.flatMap(pos =>
          pos.getImpsList.map(imp =>
            ((pv.getPage.getSiteId, pv.getIdLocators.getQqOpenid.toStringUtf8), imp.getAd.getQualityProductId, imp.getPingTime)
          )
        )
      )
      .groupBy(_._1)
      //    v=((site_id, qq_open_id), [((site_id, qq_open_id), quality_product_id, ping_time)])
      .map(v => {
        //qualityProductIdList, sort by ping_time
        val qPList = v._2.toList.sortBy(_._3).map(v => (v._2))
        var repeatedNum = 0
        val windowSize = 10
        for (i <- qPList.indices) {
          var windowSizeQpList = List[String]()
          if (i > windowSize) {
            windowSizeQpList = qPList.slice(i - windowSize, i)
          } else {
            windowSizeQpList = qPList.slice(0, i)
          }
          if (windowSizeQpList.contains(qPList(i))) {
            repeatedNum += 1
          }
        }
        //site_id, qq_open_id, n, p
        (v._1._1, v._1._2, qPList.length, repeatedNum)
      }).groupBy(v => (v._1, v._3, v._4)).map(v => (v._1._1, v._1._2, v._1._3, v._2.toList.length))
  }

  def main(args: Array[String]) {
    val cmdArgs = Common.parseArgs(args)
    val hour = cmdArgs.getOrElse("hour", "20210511")
    val inputPath = cmdArgs.getOrElse("inputPath", s"hdfs://ss-cdg-cold1-v2/data/MAPREDUCE/CDG/g_sng_gdt_gdt_log_process/threemonth/gdt/dragon/joined_exposure/" + hour + "*/*")
    val outputPath = cmdArgs.getOrElse("outputPath", s"hdfs://ss-cdg-13-v2/data/PIG/CDG/g_sng_gdt_gdt_ranking/7days/miraclemao/test_task/max_exposure_rate/" + hour)


    val sparkConf = new SparkConf().setAppName("testdemo")
    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_miraclemao:tdw_miraclemao")
    val sc = new SparkContext(sparkConf)
    Common.deletePath(sc, outputPath)

    val DRAGON_FIELDNAMES = List("page.site_id",
      "id_locators.qq_openid",
      "positions.imps.ad.quality_product_id",
      "positions.imps.ping_time"
    ).mkString(",")
    sc.hadoopConfiguration.set("dragon.reader.fieldnames", DRAGON_FIELDNAMES)

    val data = getMaxExposureRateCountQQ(sc, inputPath).map(v => v.productIterator.mkString("\t"))

    data.repartition(1).saveAsTextFile(outputPath)
  }

}
