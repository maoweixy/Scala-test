package com.wei.mao

object ScalaDemo {
  def main(args: Array[String]): Unit = {

    //    val tuples = tuples1
    //    println(ints.slice(0,2))
    //    val yy = 2;
    //    val xx = yy.toDouble / 4
    //    println(xx)
    flowTest()
    //    listTest()
//    tupleTest()
    //    mapTest()
    //    groupByTest()
    //    reduceTest()
//        flatMapTest1()
    //    wordCount()

  }


  //  对一些流程控制Test，比如for
  def flowTest() = {
    val s = "aaa bb cc"
    val arrs = s.split(" ")
    println(arrs(1))
  }


  def listTest() = {
    val ints = List(1, 2, 3, 4, 5, 6)
    ints.toString()
    println(ints.mkString("\t"))
  }

  def tupleTest() = {
    //    val tupleList =  List((("aa", 1), "qq"), (("bb", 2), "ww"))
    val tupleList = List(("a", 1, "aa"), ("b", 2, "bb"), ("c", 3, "cc"), ("b", 2, "dd"))


    tupleList.map(m => m.productIterator.mkString("\t")).foreach(println)
    //    println(tupleList.map(m => m._1.productIterator.mkString("\t") + "\t" + m._2))
    //    tupleList.map(m => m._1.productIterator.mkString())

  }

  def mapTest() = {
    //    val ints = Map(1 -> "a", 2 -> "b")
    val ints = Map[Int, String]()
    //    ints.
    println(ints)
  }


  def groupByTest() = {
    //    val tupleList = List(("a", 1, "aa"), ("b", 2, "bb"), ("c", 3, "cc"), ("b", 2, "dd"))
    val tupleList = List(("a", 1), ("b", 2), ("c", 3), ("b", 2))
    val stringToTuples: Map[String, List[(String, Int)]] = tupleList.groupBy(_._1)

    //    tupleList.groupBy(v => (v._1, v._2)).count()
  }

  def reduceTest() = {
    val ints = List(1, 2, 3)
    println(ints.reduce((v1, v2) => v1 + v2))
  }

  def flatMapTest() = {
    val nest1 = List((1, (2, 3)), (4, (5, 6)))
    val nest2 = List((1, ((2, 3), (1, 2, 3))), (1, ((2, 3), (1, 2, 3))))

    //    val nest: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    println(nest1.map(m => ()));

    println(nest2.map(m => (m._1, m._2._1._1, m._2._1._2, m._2._2._1, m._2._2._2, m._2._2._3)))
  }

  def flatMapTest1() = {
    val nest1 = List(List(List("1", "2"), List("3", "4")), List(List("5", "6"), List("5", "6")))
    val nest2 = List(List("1", "2"), List("3", "4"))
    println("1-----")
    nest1.map(_.map(_.map(_ + "a"))).foreach(println)
    println("2-----")
    nest1.map(_.map(_.map(_ + "a"))).foreach(println)
    println("3-----")
    nest1.flatMap(_.flatMap(_.map(_ + "a"))).foreach(println)
    println("4-----")
    nest1.flatMap(_.flatMap(_.flatMap(_ + "a"))).foreach(println)
  }


  def wordCount() = {
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")
    val wordList: List[String] = stringList.flatMap(str => str.split(" "))
    println(wordList)
    wordList.groupBy(word => word).map(v => {
      v
    })
  }
}
