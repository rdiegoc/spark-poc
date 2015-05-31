package exeo.spark_poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.codehaus.jackson.map.ObjectMapper


/**
 * @author DIEGO
 */
object SApp extends scala.App {
  val mapper = new ObjectMapper();
  
  val conf = new SparkConf().setAppName("recargas").setMaster("local[5]")
  val ssc = new StreamingContext(conf, Seconds(15))
  
  val topics = Map("test-topic" -> 1)
  val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", topics)
  
  val recarga = kafkaStream.map(x => (mapper.readValue(x._2, classOf[Recarga])))
  
  val pairs = recarga.map( r => (r.getIdMayorista, r))
  
  def sumatoriaMontoReducer(r1: Recarga, r2: Recarga):Recarga = {
    val recarga:Recarga = new Recarga()
    recarga.setIdMayorista(r1.getIdMayorista)
    recarga.setMontoRecarga( r1.getMontoRecarga.add(r2.getMontoRecarga) )
    
    return recarga
  }
  
  val reduceSumMonto = pairs.reduceByKey( (r1, r2) => sumatoriaMontoReducer(r1, r2))
  reduceSumMonto.print()
  
  val counts = recarga.map( r => (r.getIdMayorista, 1L))
  val reduceCount = counts.reduceByKey(_ + _)
  reduceCount.print()
  
  val reduceMaxFecha = pairs.reduceByKey( (r1, r2) => if(r1.getFechaTransTp.getTime > r2.getFechaTransTp.getTime) r1 else r2 )
  reduceMaxFecha.print()
  
  ssc.start();
  ssc.awaitTermination();
  
}