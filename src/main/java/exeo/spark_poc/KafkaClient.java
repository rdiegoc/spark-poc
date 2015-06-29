package exeo.spark_poc;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import scala.collection.mutable.StringBuilder;

public class KafkaClient {
	
	private static Map<Long, Triple<Long, Double, Date>> byMayorista = new HashMap<Long, Triple<Long, Double, Date>>(); 
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "192.168.56.10:2181");
//		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.56.10:9092");
//		props.put("metadata.broker.list", "localhost:9092");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		int n = 0;
		for(;;) {
			String json = createJson();
			System.out.println("n = " + n++ + " -> json: " + json);
			printAcum();
			
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

	private static void printAcum() {
		System.out.println(byMayorista);
	}

	private static String createJson() {
		long idMayorista = RandomUtils.nextLong(1, 5);
		double monto = RandomUtils.nextDouble(1, 200);
		Date fechaRecarga = new Date();
		
		if(byMayorista.containsKey(idMayorista)) {
			Triple<Long, Double, Date> triple = byMayorista.get(idMayorista);
			
			long cantAcum = (Long)triple.getLeft();
			double montoAcum = (Double)triple.getMiddle();
			Date maxFechaAcum = (Date)triple.getRight();
			
			triple = new ImmutableTriple<Long, Double, Date>(++cantAcum, montoAcum + monto, (fechaRecarga.after(maxFechaAcum) ? fechaRecarga : maxFechaAcum));
			byMayorista.put(idMayorista, triple);
		} else  {
			Triple<Long, Double, Date> triple = new ImmutableTriple<Long, Double, Date>(1L, monto, fechaRecarga);
			byMayorista.put(idMayorista, triple);
		}
		
		return new StringBuilder("{\"idMayorista\" : \"")
			.append(idMayorista).append("\",\"fechaTransTp\" : \"")
			.append(DateFormat.getDateTimeInstance().format(fechaRecarga)).append("\",\"montoRecarga\" : ")
			.append(monto).append("}")
			.toString();
	}

}
