package com.wei.mao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdbank.{TDBankProvider, TubeReceiverConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sharkdtu
 * 2015/9/9
 */
object SparkTest1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    println("hello")
    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("appName")
      .getOrCreate()
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (1, "asf", 2.0),
      (2, "2143", 3.0),
      (3, "rfds", 4.0),
      (4, null, 5.0),
      (5, "", Double.NaN)
    )).toDF("label", "sentence", "double_value")

    sentenceDataFrame.show()
    sentenceDataFrame.na.drop().show()
  }
}
