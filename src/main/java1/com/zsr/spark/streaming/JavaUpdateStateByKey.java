package java1.com.zsr.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
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
import java.util.List;

/**
 * @author: zhouyi
 * @create: 2019-04-09 20:25:03
 **/
public class JavaUpdateStateByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("abc");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(4));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9888);
        JavaPairDStream<String, Integer> pairDstream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> result = pairDstream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                int totalValue = 0;
                if (v2.isPresent()) {
                    totalValue += v2.get();
                }
                for (Integer num : v1) {
                    totalValue += num;
                }
                return Optional.of(totalValue);
            }
        });
        result.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
        }
        jsc.stop();
    }
}
