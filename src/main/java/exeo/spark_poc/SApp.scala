package exeo.spark_poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.data.redis.core.RedisTemplate


/**
 * @author DIEGO
 */
object SApp extends scala.App {
  val mapper = new ObjectMapper();
  val redisTemplate = RedisConnectionManager.getTemplate
  
  val conf = new SparkConf().setAppName("recargas").setMaster("local[5]")
  val ssc = new StreamingContext(conf, Seconds(20))
  
  val topics = Map("test-topic" -> 1)
  val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", topics)
  
  val recarga = kafkaStream.map(x => (mapper.readValue(x._2, classOf[Recarga])))
  
  val sumMonto = recarga.map( r => (r.getIdMayorista, r.getMontoRecarga))
  val reduceSumMonto = sumMonto.reduceByKey( (m1, m2) => m1.add(m2) )
  reduceSumMonto.foreachRDD( rdd =>
    rdd.foreach {
      record => {
        println(record._1 + ":" + record._2.doubleValue())
        redisTemplate.opsForHash().increment(KeyUtils.generateKey(record._1), "sumMonto", record._2.doubleValue());
      }
    }
  )
  
  val counts = recarga.map( r => (r.getIdMayorista, 1L))
  val reduceCount = counts.reduceByKey(_ + _)
  reduceCount.foreachRDD( rdd =>
    rdd.foreach {
      record => {
        redisTemplate.opsForHash().increment(KeyUtils.generateKey(record._1), "recargas", record._2)
      }
    }
  )
  
  val pairs = recarga.map( r => (r.getIdMayorista, r.getFechaTransTp.getTime))
  val reduceMaxFecha = pairs.reduceByKey( (f1, f2) => if(f1 > f2) f1 else f2 )
  reduceMaxFecha.foreachRDD( rdd =>
    rdd.foreach {
      record => {
        redisTemplate.opsForHash().put(KeyUtils.generateKey(record._1), "maxFecha", String.valueOf(record._2));
      }
    }
  )
  
  ssc.start();
  ssc.awaitTermination();
  
}