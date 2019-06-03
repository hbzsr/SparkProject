package java1.com.zsr.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;


/**
 * @author: zhouyi
 * @create: 2019-04-01 14:36:44
 **/
public class CommonJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("java-spark-demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> words = sc.textFile("words");
        JavaRDD<String> rdd = words.flatMap((String s) -> {
            String[] sArray = s.split(" ");
            return Arrays.asList(sArray).iterator();
        });
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair((elem) -> {
            return new Tuple2<>(elem, 1);
        });
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(((v1, v2) -> {
            return v1 + v2;
        }));
        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("~~~" + stringIntegerTuple2);
            }
        });

//        /*
//     map / flatMap / filter / foreach / sortByKey / sortBy / groupBy / groupByKey / reduceByKey
//     / first / take / collect / count / sample / reduce
//     */

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(new String[]{"1", "2", "3", "4", "23"}));
        System.out.println(rdd1.count());
        rdd1.sortBy(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.parseInt(v1);
            }
        }, false, 3);
        rdd1.first();
        rdd1.take(0);
        rdd1.collect();
        rdd1.sample(true, 0.3, 1l);
        sc.stop();
    }
}
