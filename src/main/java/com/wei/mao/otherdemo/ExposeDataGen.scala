//package com.wei.mao.otherdemo
//
//import DataGenTool._
//import com.gdt.dragon.DragonInputFormat
//import com.gdt.proto.GDTCommon.ExpTriggerInfo.TriggerKey
//import com.gdt.proto.GDTDevice.Device
//import com.gdt.proto.GDTPageview.{Impression, Pageview, Position}
//import inco.common.log_process.{Common, LogProcess}
//import org.apache.hadoop.io.{BytesWritable, NullWritable}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
//import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.{SparkConf, SparkContext}
//import qzap.targeting.UserIdProto.IdLocatorRecord
//import xq.reward_video.utils.IdType
//
//import scala.collection.JavaConversions.asScalaBuffer
//import scala.collection.mutable.ArrayBuffer
//
//object ExposeDataGen {
//  val needColumns = EXPOSE_COLS.slice(0, EXPOSE_COLS.length - 4)
//
//  def getExpData(position: Position, targetExpIds:Array[Int]): Array[String] = {
//    // Get exp info
//    val resExpId = getTargetExpid(position, targetExpIds)
//    val placementTypeTriggers = position.getExpTriggerInfoList.filter(_.getTriggerKey == 9).map(_.getValue)
//    var placementType:Int = -1
//    if(placementTypeTriggers.nonEmpty) placementType = placementTypeTriggers.head
//
//    Array(resExpId, placementType.toString)
//  }
//  def genUserFeature(pageview: Pageview): Array[String] = {
//    val userInfo = pageview.getUserInfo
//    val deviceProfile = userInfo.getDevicePreference
//    val brandId = deviceProfile.getBrandId.toString
//    val osVersion = pageview.getDevice.getOsVersion
//
//    val simplifiedUserInfo = pageview.getSimplifiedUserInfo
//    val age = simplifiedUserInfo.getAge.toString
//    val gender = simplifiedUserInfo.getGender.toString
//    val education = simplifiedUserInfo.getEducation.getNumber.toString
//
//    val sdkVersion = pageview.getDevice.getSdkVersion
//    val appVersion = pageview.getPage.getAppVersion
//
//    Array(
//      brandId, osVersion,
//      age, gender, education,
//      sdkVersion, appVersion
//    )
//  }
//  def getAdData(imp:Impression): Array[String] = {
//    val ad = imp.getAd
//
//    val adgroupId = ad.getAdgroupId.toString
//    val adType = ad.getAdType.toString
//
//    // Video
//    val videoStyleId = ad.getReranking.getVideoStyleId.toString
//    val videoDuration = ad.getVideoDuration.toString
//
//
//    // Get ecpm info
//    val pctr = ad.getReranking.getPctr.toString
//    val pcvr = ad.getPcvr.getSmartPcvr.toString
//    val ecpm = ad.getReranking.getTotalEcpm.toString
//
//    // Creative
//    val creativeId = ad.getCreativeId.toString
//    val creativeSize = ad.getCreativeSize.toString
//
//    // Vertical
//    val verticalIds = ad.getVerticalList.map(_.getId).mkString(",")
//
//    // Product
//    val productType = ad.getProductType.getNumber.toString
//
//    Array(
//      adgroupId, adType,
//      videoStyleId, videoDuration,
//      pctr, pcvr, ecpm,
//      creativeId, creativeSize,
//      verticalIds,
//      productType
//    )
//  }
//
//  def getUserId(idLocator:IdLocatorRecord): String = {
//    val idFromMapping = idLocator.getIdFromMapping
//    if (idLocator.hasMediaSpecifiedId) {
//      return idLocator.getMediaSpecifiedId.toStringUtf8
//    }
//    if (idLocator.hasGuid && (idFromMapping & (1 << IdType.ID_LOCATOR_TYPE_GUID)) == 0) {
//      return Common.bin2Hex(idLocator.getGuid)
//    }
//    if (idLocator.hasWuid && (idFromMapping & (1 << IdType.ID_LOCATOR_TYPE_WUID)) == 0) {
//      return idLocator.getWuid.toStringUtf8
//    }
//    if (idLocator.hasQqOpenid) {
//      return idLocator.getQqOpenid.toStringUtf8
//    }
//    if (idLocator.hasMd5Imei && (idFromMapping & (1 << IdType.ID_LOCATOR_TYPE_IMEI)) == 0) {
//      return Common.bin2Hex(idLocator.getMd5Imei)
//    }
//    if (idLocator.hasMd5Idfa && (idFromMapping & (1 << IdType.ID_LOCATOR_TYPE_IDFA)) == 0) {
//      return Common.bin2Hex(idLocator.getMd5Idfa)
//    }
//    if (idLocator.hasQidfa && (idFromMapping & (1 << IdType.ID_LOCATOR_TYPE_QIDFA)) == 0) {
//      return idLocator.getQidfa.toStringUtf8.toUpperCase
//    }
//    ""
//  }
//
//
//  def genExposeData(spark: SparkSession, sc:SparkContext, dayno:String, siteIds:Array[Long], targetExpIds:Array[Int]): Dataset[Row] = {
//    val targetExpIdsBroadcast = sc.broadcast(targetExpIds)
//
//    val resRdd = LogProcess.getExposureLog(sc, dayno).map(_._1).filter(v => siteIds.contains(v.getPage.getSiteId)).flatMap((pageView:Pageview) => {
//      val targetExpIds = targetExpIdsBroadcast.value
//
//      val id = pageView.getId
//      val siteId = pageView.getPage.getSiteId.toString
//      val qq = if (pageView.getIdLocators.hasEncryptedQq) toHexString(pageView.getIdLocators.getEncryptedQq.toByteArray) else pageView.getIdLocators.getQqOpenid.toStringUtf8
//      val openId = pageView.getIdLocators.getQqOpenid.toStringUtf8
//      val loginOpenId = pageView.getLoginOpenid
//      val mediaSpecifiedId = pageView.getIdLocators.getMediaSpecifiedId.toStringUtf8
//      val userData = genUserFeature(pageView)
//
//      pageView.getPositionsList.flatMap { position =>
//        val positionId = position.getId.toString
//        val expData = getExpData(position, targetExpIds)
//
//        // Get qq public info
//        val qqArticleId = position.getQqPublicInfo.getQqPublicArticleId.toString
//
//        // reward video info
//        val rewardTime = position.getRewardTime.toString
//
//        getImpression(position).map { imp =>
//          val traceId = imp.getId
//          val pingTime = imp.getPingTime.toString
//          val adData = getAdData(imp)
//          val cols = Array.concat(Array(id, traceId, positionId, pingTime, qq, openId, loginOpenId, mediaSpecifiedId, qqArticleId, rewardTime),
//            expData, adData, userData, Array(siteId))
//          Row(cols:_*)
//        }
//      }
//    })
//    spark.createDataFrame(resRdd, StructType(Array.concat(needColumns, Array("site_id")).map(v => StructField(name=v, dataType=StringType))))
//  }
//  def genCpmData(spark: SparkSession, sc:SparkContext, dayno:String, siteIds:Array[Long]): Dataset[Row] = {
//    val pvRdd = sc.newAPIHadoopFile[NullWritable, BytesWritable, DragonInputFormat](CPM_PATH_PREFIX + dayno + "*/*")
//      .map(_._2.copyBytes()).map(bytes => Pageview.parseFrom(bytes))
//      .filter{pv => siteIds.contains(pv.getPage.getSiteId)}
//    val resRdd = pvRdd.flatMap(pv => {
//      val id = pv.getId
//      pv.getPositionsList.flatMap { position =>
//        getImpression(position).map { imp =>
//          val traceId = imp.getId
//          val cost = imp.getBillCpm.getRealCostMicros64.toFloat / 1e6.toFloat
//          val optimization_goal = if (imp.getAd.hasSmartOptimizer) imp.getAd.getSmartOptimizer.getOptimizationGoal else 0
//          val exposeGmv = if (optimization_goal == 0) cost else 0.0F
//          Row(id, traceId, cost.toString, exposeGmv.toString)
//        }
//      }
//    })
//    val resData = spark.createDataFrame(resRdd, StructType(Array("id", "trace_id", "expose_cost", "expose_gmv").map(v => StructField(name=v, dataType=StringType))))
//
//    resData
//  }
//  def genVideoPlayData(spark:SparkSession, sc:SparkContext, dayno:String, siteIds:Array[Long]):Dataset[Row] = {
//    if(ifExist(sc, VIEDO_PLAY_PREFIX + dayno + "/_SUCCESS")) {
//      val pvRdd = sc.newAPIHadoopFile[NullWritable, BytesWritable, DragonInputFormat](VIEDO_PLAY_PREFIX + dayno + "/*")
//        .map(_._2.copyBytes()).map(bytes => Pageview.parseFrom(bytes))
//        .filter{pv => siteIds.contains(pv.getPage.getSiteId)}
//      val resRdd = pvRdd.flatMap(pv => {
//        val id = pv.getId
//        pv.getPositionsList.flatMap { position =>
//          getImpression(position).map { imp =>
//            val traceId = imp.getId
//            val videoView = imp.getVideoView
//            val finishType = videoView.getFinishType
//            var playEndTime = videoView.getPlayEndTime
//            if(playEndTime > 1000) playEndTime = playEndTime / 1000
//            Row(id, traceId, playEndTime, finishType)
//          }
//        }
//      })
//      var resData = spark.createDataFrame(resRdd, StructType(Array(
//        StructField(name="id", dataType=StringType), StructField(name="trace_id", dataType=StringType),
//        StructField(name="play_time", dataType=IntegerType), StructField(name="finish_type", dataType=IntegerType)
//      )))
//      val exposeData = resData.groupBy("id", "trace_id").agg(max("play_time").as("play_time"))
//        .na.drop().withColumn("play_time", col("play_time").cast("string"))
//      val clickData = resData.filter(col("finish_type").equalTo(2)).groupBy("id", "trace_id").agg(max("play_time").as("click_play_time"))
//        .na.drop().withColumn("click_play_time", col("click_play_time").cast("string"))
//      resData = exposeData.join(clickData, Array("id", "trace_id"), "outer")
//
//      resData
//    } else null
//  }
//
//  def genData(spark: SparkSession, sc:SparkContext, dayno:String, siteIds:Array[Long], targetExpIds:Array[Int], user:String): Unit = {
//    val exposeData = genExposeData(spark, sc, dayno, siteIds, targetExpIds)
//    val cpmData = genCpmData(spark, sc, dayno, siteIds)
//    val videoPlayData = genVideoPlayData(spark, sc, dayno, siteIds)
//    var resData = exposeData.join(cpmData, Array("id", "trace_id"), "left_outer")
//    if(videoPlayData != null) {
//      resData = resData.join(videoPlayData, Array("id", "trace_id"), "left_outer")
//    } else {
//      resData = resData.withColumn("play_time", lit("0")).withColumn("click_play_time", lit("0"))
//    }
//    resData = resData.na.fill(defaultGen(EXPOSE_COLS)).persist(StorageLevel.DISK_ONLY)
//
//    for (i <- siteIds.indices) {
//      val siteId = siteIds(i).toString
//      resData.filter(col("site_id").equalTo(siteId)).select(EXPOSE_COLS.map(col):_*).repartition(10)
//        .write.format("parquet").mode("overwrite").save(s"hdfs://ss-cdg-3-v2/user/${user}/online/expose/${siteId}/${dayno}")
//    }
//    resData.unpersist()
//  }
//
//  def main(args: Array[String]): Unit = {
//    val cmdArgs = Common.parseArgs(args)
//    val dayno = cmdArgs.getOrElse("dayno", "20200822")
//    val siteIds = cmdArgs.getOrElse("site_ids", "").split(",").map(_.toLong)
//    val user = cmdArgs.getOrElse("user", "tdw_alphatan")
//
//    val sparkConf = new SparkConf().setAppName("expose_data_gen")
//    sparkConf.set("spark.hadoop.hadoop.job.ugi", "tdw_alphatan:tdw_alphatan")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val sc = new SparkContext(sparkConf)
//    sc.hadoopConfiguration.set("dragon.reader.fieldnames", DRAGON_FIELDNAMES)
//
//    val spark = new SQLContext(sc).sparkSession
//
//    genData(spark, sc, dayno, siteIds, TARGET_EXP_IDS, user)
//  }
//}
