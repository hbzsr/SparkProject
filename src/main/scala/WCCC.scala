/**
  * @author: zhouyi
  * @create: 2019-05-14 21:33:36
  **/
import org.apache.spark.{SparkConf, SparkContext}
object WCCC {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("WCC").setMaster("local")
    val ss = new SparkContext(sc)
    val rdd = ss.parallelize(List("hello","hello","word","word","c","d"))
    
  }

}
