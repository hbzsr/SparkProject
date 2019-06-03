package scala.com.zsr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author: zhouyi
  * @create: 2019-04-09 23:07:23
  **/
object WindowOperator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("windowOperator")
    val ssc = new StreamingContext(sc, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9888)
    val words: DStream[String] = lines.flatMap(one => {
      one.split(" ")
    })
    val pairedWords: DStream[(String, Int)] = words.map(one => {
      (one, 1)
    })
    // val result: DStream[(String, Int)] = pairedWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2,Durations.seconds(15),Durations.seconds(5))
    //优化实现reduceByKeyAndWindow ， 去掉之前的，加上新增的，必须保存上一个window ds 所以必须设置checkpoint
    ssc.checkpoint("./checkpoint")
    val result: DStream[(String, Int)] = pairedWords.reduceByKeyAndWindow(
      _ + _,
      _ - _,
      Durations.seconds(15),
      Durations.seconds(5)
    )
    //自定义
//    val dsRdd: DStream[(String, Int)] = pairedWords.window(Durations.seconds(15),Durations.seconds(5))
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
