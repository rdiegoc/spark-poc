package exeo.spark_poc

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import org.apache.spark.rdd.RDD

/**
 * @author DIEGO
 */
object Redis {
  
  var redisPool : JedisPool = null;
  
  def savePartition[K, V] (partitionOfRecords : Iterator[(K, V)], callback: (Jedis, (K, V)) => Unit) = {
    if(!partitionOfRecords.isEmpty) {
      val redis = getConnection()
  
      partitionOfRecords.foreach(record => callback(redis, record) )
  
      closeConnection(redis)
    }
  }
  
  def save[K, V] (rdd : RDD[(K, V)], callback: (Jedis, (K, V)) => Unit) = {
    if(!rdd.isEmpty) {
      rdd.foreachPartition { partitionOfRecords => {
        val redis = getConnection()
        
        partitionOfRecords.foreach(record => callback(redis, record) )
    
        closeConnection(redis)
        }
     }
    }
  }
  
  def getPool() : JedisPool = synchronized {
    if(redisPool == null) {
      val poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(100);
      poolConfig.setMinIdle(30000);
      
      redisPool = new JedisPool(poolConfig, "192.168.56.10", 6379);
    }
    
    redisPool;
  }
  
  def getConnection() : Jedis = {
    getPool().getResource
  }
  
  def closeConnection(redis : Jedis) = {
    if(redis != null)
      redis.close()
  }
  
}