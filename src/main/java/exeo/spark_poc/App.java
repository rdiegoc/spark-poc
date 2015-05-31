package exeo.spark_poc;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jackson.map.ObjectMapper;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {
	
	static ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("recargas");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("test-topic", 1);
		final JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "localhost:2181", "test-consumer-group", topicMap);
		
		JavaDStream<Recarga> recarga = messages.map(new Function<Tuple2<String, String>, Recarga>() {
			public Recarga call(Tuple2<String, String> tuple) throws Exception {
				String json = tuple._2();
				
				return mapper.readValue(json, Recarga.class);
			}
		});
		
//		JavaDStream<String> words = json.map(new FlatMapFunction<String, String>() {
//			public Iterable<String> call(String x) {
//				
//				return Arrays.asList(x.split(" "));
//			}
//		});
		
		// Count each word in each batch
		JavaPairDStream<Long, Recarga> pairs = recarga.mapToPair(new PairFunction<Recarga, Long, Recarga>() {
			public Tuple2<Long, Recarga> call(Recarga s) throws Exception {
				return new Tuple2<Long, Recarga>(s.getIdMayorista(), s);
			}
		});

//		JavaPairDStream<Long,Iterable<String>> wordCounts = pairs.groupByKey();
		JavaPairDStream<Long,Recarga> wordCounts = pairs.reduceByKey(new Function2<Recarga, Recarga, Recarga>() {
			public Recarga call(Recarga v1, Recarga v2) throws Exception {
				Recarga r = new Recarga();
				
				r.setIdMayorista(v1.getIdMayorista());
				r.setMontoRecarga(v1.getMontoRecarga().add(v2.getMontoRecarga()));
				return r;
			}
		});
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.foreach(new Function<JavaPairRDD<Long,Recarga>, Void>() {
			public Void call(JavaPairRDD<Long,Recarga> v1) throws Exception {
				// grabarlo en redis
				System.out.println(v1.keys());
				return null;
			}
		});
		
		jssc.start();
	    jssc.awaitTermination();
	}
}
