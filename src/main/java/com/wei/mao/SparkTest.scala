package com.wei.mao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    joinTest(sc)
//    groupByTest(sc)

//    pr(sc)
    //关闭 Spark 连
    sc.stop()
  }

  def joinTest(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(Array(("1","Spark"),("2","Hadoop"),("3","Scala"),("4","Java")),2)
    //建立一个行业薪水的键值对RDD，包含ID和薪水，其中ID为1、2、3、5
    val rdd2 = sc.makeRDD(Array(("1","30K"),("2","15K"),("3","25K"),("5","10K")),2)

    println("//下面做Join操作，预期要得到（1,×）、（2,×）、（3,×）")
    val joinRDD=rdd1.join(rdd2).collect.foreach(println)

    println("//下面做leftOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(4,×）")
    val leftJoinRDD=rdd1.leftOuterJoin(rdd2).collect.foreach(println)
    println("//下面做rightOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(5,×）")
    val rightJoinRDD=rdd1.rightOuterJoin(rdd2).collect.foreach(println)
  }

  def groupByTest(sc: SparkContext) = {
    val a = sc.makeRDD(List(1, 2, 3, 4))
//    a.collect().foreach(println)
        a.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect.foreach(println)

  }

  def pr(sc: SparkContext) = {
    val rdd01 = sc.makeRDD(List(1,2,3,4,5,6))
    val r01 = rdd01.map { x => x * x }
    println(r01.collect().mkString(","))
  }


  def wordCount(sc: SparkContext) = {
    var fileRDD: RDD[String] = sc.textFile("data")

    // 将文件中的数据进行分词

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    // 转换数据结构 word => (word, 1)

    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    // 将转换结构后的数据按照相同的单词进行分组聚合

    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)

    // 将数据聚合结果采集到内存中

    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    // 打印结果

    word2Count.foreach(println)
  }


}
