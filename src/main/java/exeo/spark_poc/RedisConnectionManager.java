package exeo.spark_poc;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionManager {
	
	private static JedisPool pool;
//	private static JedisConnectionFactory connectionFactory;
	
	public static synchronized JedisPool getJedis() {
		if(pool == null) {
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(100);
			poolConfig.setMinIdle(30000);
			
			pool = new JedisPool(poolConfig, "192.168.56.10", 6379);
		}
		
		return pool;
	}
	
	public static Jedis getConnection() {
		return getJedis().getResource();
	}
	
//	public static RedisTemplate<String, String> getTemplate() {
//		if(connectionFactory == null) {
//			connectionFactory = new JedisConnectionFactory();
//			connectionFactory.setHostName("localhost");
//			connectionFactory.setPort(6379);
//			connectionFactory.setUsePool(true);
//			
//			JedisPoolConfig poolConfig = new JedisPoolConfig();
//			poolConfig.setMaxTotal(100);
//			poolConfig.setMinIdle(30000);
//			connectionFactory.setPoolConfig(poolConfig);
//			
//			connectionFactory.afterPropertiesSet();
//		}
//		
//		RedisTemplate<String, String> template = new RedisTemplate<String, String>();
//		template.setConnectionFactory(connectionFactory);
//		template.afterPropertiesSet();
//		template.setKeySerializer(new StringRedisSerializer());
//		template.setHashValueSerializer(new StringRedisSerializer());
//		template.setHashKeySerializer(new StringRedisSerializer());
//		
////		template.setEnableTransactionSupport(true);
//		
//		return template;
//	}
	
}
