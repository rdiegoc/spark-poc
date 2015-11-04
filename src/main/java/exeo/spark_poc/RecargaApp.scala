package exeo.spark_poc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import tp.support.misc.JsonConverterGsonImpl
import java.math.BigDecimal
import org.apache.spark.streaming.flume.FlumeUtils

/**
 * @author DIEGO
 */
object RecargaApp extends scala.App {

  // Inicia los contextos de spark
  val conf = new SparkConf().setAppName("recargas").setMaster("local[5]")
  val ssc = new StreamingContext(conf, Seconds(Config.Spark.microBatchingTime))

  // Configuracion de kafka
  val topics = Set(Config.Kafka.topic)
  val kafkaParams = Map( ("metadata.broker.list", Config.Kafka.brokerList), ("group.id",Config.Kafka.groupId) )

  // Lectura de mensajes desde kafka
  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//  val stream = FlumeUtils.createStream(ssc, "192.168.56.1", 7777);
  
  // Transforma el JSON con la recarga en un objecto que pueda ser manipulado
  val recarga = stream.map(x => {
//      RolUtils.convertToRecarga(new String(x.event.getBody.array()))
      RolUtils.convertToRecarga(x._2)
  });
  
  // transforma el objecto en un tupla
  val dataByMayorista = recarga.map( r => (r.idMayorista, (r.montoRecarga, 1L, r.fechaTransTp.getTime)) )
  // reduce calculando los indicadores por key (idMayorista)
  val reduce = dataByMayorista.reduceByKey( (t1, t2) => (t1._1.add(t2._1), t1._2 + t2._2, if(t1._3 > t2._3) t1._3 else t2._3) ).cache()
  // por cada mayorista, graba los datos
  reduce.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, (BigDecimal, Long, Long))) => {
    val idMayorista = record._1;

    val sumMonto = record._2._1;
    val cantidad = record._2._2;
    val fecha = record._2._3;
    
    val key = RolUtils.generateKey(idMayorista)
    val keyWithSchema = RolUtils.setKeyAndSchema(redis, key)

    redis.sadd("s:rol:" + RolUtils.generateKeyWithDate(), key);
    
    redis.hincrByFloat(keyWithSchema, "sumMonto", sumMonto.doubleValue())
    redis.hincrBy(keyWithSchema, "recargas", cantidad)
    redis.hset(keyWithSchema, "maxFecha", String.valueOf(fecha))
   })
  )
  
  // Crea una nueva tupla con solo los montos y la cantidad de recargas
  val all = reduce.map( t => ( t._2._1, t._2._2 ) )
  // reduce la tupla sumarizando
  val reduceAll = all.reduce( (t1, t2) => ( (t1._1.add(t2._1), (t1._2 + t2._2)) ) )
  // graba los datos
  reduceAll.foreachRDD( rdd => Redis.save(rdd, (redis, record : (BigDecimal, Long)) => {
    val sumMontoTotal = record._1;
    val cantidad = record._2;
    
    var key = RolUtils.generateKeyWithDate()
    var keyWithSchema = RolUtils.setKeyAndSchema(redis, key)
    
    redis.hincrByFloat(keyWithSchema, "sumMonto", sumMontoTotal.doubleValue())
    redis.hincrBy(keyWithSchema, "recargas", cantidad)
    
    key = RolUtils.generateKeyWithDateTime()
    keyWithSchema = RolUtils.setKeyAndSchema(redis, key)
    
    redis.hincrByFloat(keyWithSchema, "sumMonto", sumMontoTotal.doubleValue())
    redis.hincrBy(keyWithSchema, "recargas", cantidad)
   }))
  
//  val reduceSumMonto = sumMonto.reduceByKey( (m1: BigDecimal, m2: BigDecimal) => m1.add(m2)).cache()
//  val reduceSumMontoTotal = reduceSumMonto.reduce( (t1, t2) => (0L, t1._2.add(t2._2)) );
//  reduceSumMonto.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, BigDecimal)) => {
//     redis.hincrByFloat(Utils.generateKey(record._1), "sumMonto", record._2.doubleValue())
//   })
//  )
  
  // SIN USO DE CLASES UTILITARIAS
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
  
//  val counts = recarga.map( r => (r.idMayorista, 1L))
//  val reduceCount = counts.reduceByKey(_ + _)
//  reduceCount.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, Long)) => {
//     redis.hincrBy(Utils.generateKey(record._1), "recargas", record._2)
//   })
//  )
//  val joined = reduceCount.join(reduceSumMonto)
  
  // GRABA POR PARTICION
//  reduceCount.foreachRDD( rdd =>
//    rdd.foreachPartition { partitionOfRecords => Redis.savePartition(partitionOfRecords, (redis, record:(Long, Long)) => {
//      redis.hincrBy(KeyUtils.generateKey(record._1), "recargas", record._2)
//    }) }
//  )
  
//  val pairs = recarga.map( r => (r.idMayorista, r.fechaTransTp.getTime))
//  val reduceMaxFecha = pairs.reduceByKey( (f1, f2) => if(f1 > f2) f1 else f2 )
//  reduceMaxFecha.foreachRDD( rdd => Redis.save(rdd, (redis, record : (Long, Long)) => {
//     redis.hset(Utils.generateKey(record._1), "maxFecha", String.valueOf(record._2))
//   })
//  )
//  val reduce = joined.join(reduceMaxFecha).map{ case(idMayorista, ((cantidad, monto), maxFecha)) => ((idMayorista), (cantidad, monto, maxFecha)) }
  

  ssc.start();
  ssc.awaitTermination();
  
}