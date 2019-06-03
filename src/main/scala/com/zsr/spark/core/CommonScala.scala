package com.zsr.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @author: zhouyi
  * @create: 2019-04-01 09:43:07
  **/
object CommonScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("spark-core-demo")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("Error")
    /*
     map / flatMap / filter / foreach / sortByKey / sortBy / groupBy / groupByKey / reduceByKey
     / first / take / collect / count / sample / reduce
     */
    //    val rdd = sc.textFile("words")
    //word count
    //    rdd.flatMap(one => one.split(" ")).map(ele => (ele, 1)).reduceByKey(_ + _).sortBy(a => {
    //      a._2
    //    }).foreach(println)

    //    val rdd2 = sc.makeRDD(1 to 20)
    //    val rdd2Map = rdd2.map(a => if (a % 2 == 0) ("单数", a) else ("双数", a))
    //    rdd2Map.foreach(println)
    //    rdd2Map.groupByKey().foreach(println)
    //    val rdd3 = sc.makeRDD(Array(10, 2, 30, 44, 2, 3, 1, 90, 7, 5, 46))
    //    //    rdd3.sortBy(a => a).foreach(println)
    //    val rdd4 = rdd3.map(a => (a, a))
    //    rdd4.foreach(println)
    //    println
    //    rdd4.sortByKey().foreach(println)
    //    rdd3.first()
    //    rdd3.take(2)
    //    rdd3.count()
    //    rdd3.sample(true, 0.8, 1L).foreach(println)
    //    println()
    //    rdd3.sample(true, 0.5, 1L).foreach(println)

    val rdd11 = sc.makeRDD(Array(1, 2, 3, 4, 6, 5, 10, 7, 8, 9))
    val rdd12 = sc.makeRDD(Array(2, 4, 8, 10, 12))
    //    //    val rdd13 = sc.makeRDD(Array("a","b","c"))
    //    //    val unionRDD = rdd11.union(rdd12)
    //    //    unionRDD.foreach(println)
    //    val intersectionRDD = rdd11.intersection(rdd12)
    //    intersectionRDD.foreach(println)
    //
    //    val rdd14 = sc.makeRDD(Array(2, 2, 4, 3, 4))
    //    rdd14.distinct().foreach(println)
    //join、leftOuterJoin、rightOuterJoin
    val rdd15 = sc.makeRDD(Array(1, 2, 3, 3, 4, 4))
    //.map(a => (a, a + "~"))
    val rdd16 = sc.makeRDD(Array(4, 2, 3, 5, 5))
    //.map(a => (a, a + "X"))
    //    val joinRDD = rdd15.cartesian(rdd16)
    //    joinRDD.foreach(println)
    rdd15.mapPartitions(v => {
      val ab = ArrayBuffer[Int]()
      while (v.hasNext) {
        val next = v.next()
        print(next + " /// ")
        ab += next
      }
      println()
      ab.iterator
    }).foreach(a => println(a + ">>"))

    rdd16.mapPartitionsWithIndex((index, iter) => {
      val ab = ArrayBuffer[Int]()
      while (iter.hasNext) {
        val next = iter.next()
        print(next + " / " + index + " / ")
        ab += next
      }
      println()
      ab.iterator
    })
    //    class CustomIterator(iter: Iterator[T], func: (T => U)) extends Iterator[U] {
    //
    //      override def hasNext: Boolean = {
    //        iter.hasNext
    //      }
    //
    //      override def next(): U = {
    //        val next = iter.next()
    //        this.func(next)
    //      }
    //    }

    //    val partitionRDD = rdd15.mapPartitions(v => new CustomIterator(v, (_ + _)))
    //    partitionRDD.foreach(println)


    val rdd17 = sc.makeRDD(Array(1, 2, 3, 3, 4, 4, 1, 2, 3, 3, 4, 4, 1, 2, 3, 3, 4, 4))
    rdd17.cache()
    val i = rdd17.aggregate(0)((_ + _), (_ * _))
    println(i)
    sc.stop()
  }

}
