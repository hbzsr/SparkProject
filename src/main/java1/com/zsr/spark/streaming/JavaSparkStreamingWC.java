package java1.com.zsr.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author: zhouyi
 * @create: 2019-04-09 19:33:23
 **/
public class JavaSparkStreamingWC {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[2]").setAppName("java-wc");
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("Error");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9888);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairWord = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> result = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        result.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.stop();


    }
}
