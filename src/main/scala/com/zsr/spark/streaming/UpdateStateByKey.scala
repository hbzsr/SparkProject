package scala.com.zsr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author: zhouyi
  * @create: 2019-04-09 20:10:17
  **/
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val ss = new SparkConf().setMaster("local[2]").setAppName("updateStateByKey")
    val ssc = new StreamingContext(ss, Durations.seconds(10))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9888)
    val words: DStream[String] = lines.flatMap(one => {
      one.split(" ")
    })
    val pairedWords: DStream[(String, Int)] = words.map(one => {
      (one, 1)
    })
    ssc.checkpoint("./data/ssCheckPoint")
    val result: DStream[(String, Int)] = pairedWords.updateStateByKey((currentSeq: Seq[Int], option: Option[Int]) => {
      var totalValue = 0
      if (!option.isEmpty) {
        totalValue += option.get
      }
      for (num <- currentSeq) {
        totalValue += num
      }
      Option[Int] {
        totalValue
      }
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
