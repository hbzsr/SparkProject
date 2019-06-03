package scala.com.zsr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author: zhouyi
  * @create: 2019-04-10 09:35:40
  **/
object TranformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("transform")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val blackList: Broadcast[List[String]] = ssc.sparkContext.broadcast(List[String]("a1", "a2"))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9888)
    val pairLines: DStream[(String, String)] = lines.map(one => {
      (one.split(" ")(1), one)
    })

    /**
      * transform 算子可以拿到DStream中的RDD,对RDD使用RDD的算子操作，但最后要返回RDD，并封装成一个DStream
      * transform算子中拿到RDD的算子外，代码是在Driver端执行的，可以做到动态改变广播变量
      */
    val result: DStream[String] = pairLines.transform((pairRDD: RDD[(String, String)]) => {
      println("~~~~~~~~~~~ Driver code ~~~~~~~~~~~~")
      val filterRDD: RDD[(String, String)] = pairRDD.filter(item => {
        val nameList = blackList.value
        !nameList.contains(item._1)
      })
      filterRDD.saveAsTextFile("")
      filterRDD.map(one => one._2)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
