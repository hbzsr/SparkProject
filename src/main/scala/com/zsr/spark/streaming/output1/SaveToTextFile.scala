package scala.com.zsr.spark.streaming.output1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author: zhouyi
  * @create: 2019-04-10 10:52:03
  **/
object SaveToTextFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("savetotextfile")
    conf.setMaster("local[3]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    val lines: DStream[String] = ssc.textFileStream("src/readpath")
    val words: DStream[String] = lines.flatMap(one => {
      one.split(" ")
    })
    val paireWord = words.map(one => {
      (one, 1)
    })
//    paireWord.saveAsTextFiles("src/savepath/file", "text")
    paireWord.saveAsObjectFiles("src/savepath/file", "fi")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
