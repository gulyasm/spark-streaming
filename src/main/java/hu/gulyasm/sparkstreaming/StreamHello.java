package hu.gulyasm.sparkstreaming;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;


public class StreamHello {

    public static class ColorEvent implements Serializable {
        String id;
        Float value;
        String color;
        Long timestamp;

        @Override
        public String toString() {
            return "ColorEvent{" +
                    "id='" + id + '\'' +
                    ", value=" + value +
                    ", color='" + color + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static void main(String[] argv) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Stream hello");
        conf.setSparkHome("/opt/spark-1.6.1-bin-hadoop2.6");
        conf.setMaster("local[2]");


        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));


        JavaDStream<String> json = ssc.textFileStream("/home/gulyasm/workspace/data-stream-color/");
        JavaDStream<ColorEvent> events = json.map(new Function<String, ColorEvent>() {

            public ColorEvent call(String v1) throws Exception {
                Gson gson = new Gson();
                return gson.fromJson(v1, ColorEvent.class);
            }
        });

        JavaPairDStream<String, Float> values = events.mapToPair(new PairFunction<ColorEvent, String, Float>() {
            public Tuple2<String, Float> call(ColorEvent colorEvent) throws Exception {
                return new Tuple2<String, Float>(colorEvent.color, colorEvent.value);
            }
        });

        JavaPairDStream<String, Iterable<Float>> grouped = values.groupByKey();
        JavaPairDStream<String, Double> average = grouped.mapValues(new Function<Iterable<Float>, Double>() {
            public Double call(Iterable<Float> v1) throws Exception {
                int count = 0;
                double sum = 0;
                for (Float f : v1) {
                    count++;
                    sum += f;
                }
                return sum / count;
            }
        });

        average.print(10);


        ssc.start();
        ssc.awaitTermination();


    }

}
