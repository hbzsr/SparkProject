package java1.com.zsr.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author: zhouyi
 * @create: 2019-04-10 09:21:31
 **/
public class JavaWindowOperator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("windowOperator");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9888);
        JavaPairDStream<String, Integer> pDS = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                Iterator<String> iterator = Arrays.asList(s.split(" ")).iterator();
                return iterator;
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        pDS.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 - v2;
            }
        }, Durations.seconds(15), Durations.seconds(5));
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
