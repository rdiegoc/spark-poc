package exeo.spark_poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.BytesWritable
import tp.support.misc.JsonConverterGsonImpl

/**
 * @author DIEGO
 */
object RecargaBatch extends scala.App {
  
  val conf = new SparkConf().setAppName("recargasBatch").setMaster("local[5]")
  val sc = new SparkContext(conf);
  
  val date = RolUtils.generateKeyWithDate()
  val hdfsUrl = Config.Hdfs.url + "/" + date + "/*";
  
  // se conecta con HDFS
  val data = sc.sequenceFile(hdfsUrl, classOf[LongWritable], classOf[BytesWritable])
  // Transformas el contenido de los archivos en objetos
  val recargas = data.map( t => RolUtils.convertToRecarga(new String(t._2.copyBytes())) )
  // calcula la cantidad y la sumatoria de totales
  val result = recargas.map { recarga => (1L, recarga.montoRecarga) }.reduce( (t1, t2) => ( (t1._1 + t2._1), (t1._2.add(t2._2)) ) )
  
  // graba en redis
  Redis.hSet(date, "cantidad", result._1)
  Redis.hSet(date, "sumMonto", result._2)
  Redis.hSet(date, "promedio", result._2.doubleValue() / result._1)
  
}