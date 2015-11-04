package exeo.spark_poc

import org.apache.commons.lang3.time.DateFormatUtils
import java.util.Date
import redis.clients.jedis.Jedis
import tp.support.misc.JsonConverterGsonImpl


/**
 * @author DIEGO
 */
object RolUtils {
  
  def generateKey(idMayorista : Long) : String = {
    val format = DateFormatUtils.format(new Date(), "yyyyMMdd:HH");
    val key = new StringBuilder(format).append(":").append(idMayorista);
    return key.toString();
  }
  
  def generateKeyWithDateTime() : String = {
    val format = DateFormatUtils.format(new Date(), "yyyyMMdd:HH");
    return format;
  }
  
  def generateKeyWithDate() : String = {
    val format = DateFormatUtils.format(new Date(), "yyyyMMdd");
    return format;
  }
  
  
  def setKeyAndSchema(redis : Jedis, key : String) : String = {
    val keyWithSchema = "rol:" + key;
    
    redis.sadd("s:rol", key);
    redis.hset(keyWithSchema, "id", key)
    
    return keyWithSchema
  }
  
  def convertToRecarga(json : String) : RecargaData = {
    println(json)
    val converter = new JsonConverterGsonImpl(classOf[RecargaData])
    converter.convertJsonToObject(json)
  }
}