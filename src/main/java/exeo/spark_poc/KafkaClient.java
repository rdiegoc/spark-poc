package exeo.spark_poc;

import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;

import scala.collection.mutable.StringBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaClient {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "localhost:9092");
		
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		
		int n = 0;
		for(;;) {
			String json = createJson();
			System.out.println("n = " + n++ + " -> json: " + json);
			
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("test-topic", json);
			producer.send(data);
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static String createJson() {
		return new StringBuilder("{\"idMayorista\" : \"")
			.append(RandomUtils.nextLong(1, 5)).append("\",\"fechaTransTp\" : \"")
			.append(new Date().getTime()).append("\",\"montoRecarga\" : \"")
			.append(RandomUtils.nextDouble(1, 200)).append("\"}")
			.toString();
	}

}
