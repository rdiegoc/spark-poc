package exeo.spark_poc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import tp.support.misc.JsonConverterGsonImpl
import java.math.BigDecimal

/**
 * @author DIEGO
 */
object SApp extends scala.App {
//  val mapper = new ObjectMapper();
  
  val conf = new SparkConf().setAppName("recargas").setMaster("local[5]")
  val ssc = new StreamingContext(conf, Seconds(20))

  val topics = Set("test-topic")
  val kafkaParams = Map( ("metadata.broker.list","192.168.56.10:9092"), ("group.id","test-consumer-group") )

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  
  val recarga = kafkaStream.map(x => {
      val converter = new JsonConverterGsonImpl(classOf[RecargaData])
      converter.convertJsonToObject(x._2) 
  });

  
  val sumMonto = recarga.map( r => (r.idMayorista, r.montoRecarga))
  val reduceSumMonto = sumMonto.reduceByKey( (m1: BigDecimal, m2: BigDecimal) => m1.add(m2))

  reduceSumMonto.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, BigDecimal)) => {
     redis.hincrByFloat(KeyUtils.generateKey(record._1), "sumMonto", record._2.doubleValue())
   })
  )
  
  //  reduceSumMonto.foreachRDD( rdd =>
//    rdd.foreachPartition { partitionOfRecords => {
//        val redis = RedisConnectionManager.getConnection
//        
//        partitionOfRecords.foreach(record => 
//          redis.hincrByFloat(KeyUtils.generateKey(record._1), "sumMonto", record._2.doubleValue())
//        )
//        
//        redis.close()
//      }
//    }
//  )
  
  val counts = recarga.map( r => (r.idMayorista, 1L))
  val reduceCount = counts.reduceByKey(_ + _)
  reduceCount.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, Long)) => {
     redis.hincrBy(KeyUtils.generateKey(record._1), "recargas", record._2)
   })
  )
//  reduceCount.foreachRDD( rdd =>
//    rdd.foreachPartition { partitionOfRecords => Redis.savePartition(partitionOfRecords, (redis, record:(Long, Long)) => {
//      redis.hincrBy(KeyUtils.generateKey(record._1), "recargas", record._2)
//    }) }
//  )
  
  val pairs = recarga.map( r => (r.idMayorista, r.fechaTransTp.getTime))
  val reduceMaxFecha = pairs.reduceByKey( (f1, f2) => if(f1 > f2) f1 else f2 )
  reduceMaxFecha.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, Long)) => {
     redis.hset(KeyUtils.generateKey(record._1), "maxFecha", String.valueOf(record._2))
   })
  )

  ssc.start();
  ssc.awaitTermination();
  
}