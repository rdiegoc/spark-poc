package exeo.spark_poc

import org.apache.spark.rdd.RDD

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

/**
 * @author DIEGO
 */
object Redis {
  
  private var redisPool : JedisPool = null;
  
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
  
  def hSet(key : String, hash : String, value : Any) = {
    val redis = getConnection()
        
    redis.hset(key, hash, value.toString())
    
    closeConnection(redis)
  }
  
  private def getPool() : JedisPool = synchronized {
    if(redisPool == null) {
      val poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(Config.Redis.Pool.max);
      poolConfig.setMinIdle(Config.Redis.Pool.minIdle);
      
      redisPool = new JedisPool(poolConfig, Config.Redis.host, Config.Redis.port);
    }
    
    redisPool;
  }
  
  private def getConnection() : Jedis = {
    getPool().getResource
  }
  
  private def closeConnection(redis : Jedis) = {
    if(redis != null)
      redis.close()
  }
  
}