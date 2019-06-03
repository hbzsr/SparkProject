package scala.com.zsr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author: zhouyi
  * @create: 2019-04-10 13:55:43
  **/
object SparkStreamingHA {
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate("./checkpoint1", createStreamingContext)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def createStreamingContext(): StreamingContext = {
    val sc = new SparkConf().setAppName("ha").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")

    /**
      * 默认checkpoint 存储：
      *     1.配置信息
      *   	2.DStream操作逻辑
      *   	3.job的执行进度
      * * 4.offset
      */
    ssc.checkpoint("./checkpoint1")
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    val words: DStream[String] = lines.flatMap(line => {
      line.trim.split(" ")
    })
    val pairWords: DStream[(String, Int)] = words.map(word => {
      (word, 1)
    })
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })
    result.print()
    ssc
  }

}
