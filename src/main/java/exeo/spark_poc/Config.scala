package exeo.spark_poc

import com.typesafe.config.ConfigFactory

/**
 * @author DIEGO
 */
object Config {
  private val config =  ConfigFactory.load()
 
  object Spark {
    private val sparkConfig = config.getConfig("spark")
 
    lazy val microBatchingTime = sparkConfig.getInt("microBatchingTime")
  }
  
  object Kafka {
    private val kafkaConfig = config.getConfig("kafka")
 
    lazy val brokerList = kafkaConfig.getString("brokerList")
    lazy val groupId = kafkaConfig.getString("groupId")
    lazy val topic = kafkaConfig.getString("topic")
  }
  
  object Redis {
    private val redisConfig = config.getConfig("redis")
 
    lazy val host = redisConfig.getString("host")
    lazy val port = redisConfig.getInt("port")
    
    object Pool {
      private val poolConfig = redisConfig.getConfig("pool")
 
      lazy val max = poolConfig.getInt("max")
      lazy val minIdle = poolConfig.getInt("minIdle")
    }
  }
  
  object Hdfs {
    private val hdfsConfig = config.getConfig("hdfs")
    
    lazy val url = hdfsConfig.getString("url")
  }
}