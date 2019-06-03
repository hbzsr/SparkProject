package scala.com.zsr.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: zhouyi
  * @create: 2019-04-09 17:48:58
  **/
object SparkStreamingWC {
  def main(args: Array[String]): Unit = {
    val ss = new SparkConf().setAppName("wc").setMaster("local[2]")
    val ssc = new StreamingContext(ss, Durations.seconds(30))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9998)
    val words: DStream[String] = lines.flatMap(one => {
      one.split(" ")
    })
    val pairwords: DStream[(String, Int)] = words.map(word => {
      (word, 1)
    })
    val result: DStream[(String, Int)] = pairwords.reduceByKey((v1: Int, v2: Int) => v1 + v2)
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
