package org.chombo.storm;

import java.util.Map;

import org.chombo.util.ConfigUtility;

import redis.clients.jedis.Jedis;

/**
 * redis backed cache
 * @author pranab
 *
 */
public class RedisCache extends Cache {	
	private Jedis jedis;

	/**
	 * @param conf
	 * @param queue
	 */
	public RedisCache(Map conf) {
		String redisHost = ConfigUtility.getString(conf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(conf,"redis.server.port");
		jedis = new Jedis(redisHost, redisPort);
	}
	
	/**
	 * @param conf
	 * @param queue
	 */
	public RedisCache(Map conf, String cacheName) {
		String redisHost = ConfigUtility.getString(conf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(conf,"redis.server.port");
		jedis = new Jedis(redisHost, redisPort);
		if (null != cacheName) {
			this.cacheName = cacheName;
		}
	}

	@Override
	public void set(String key, String value) {
		if (null != cacheName) {
			jedis.hset(cacheName, key, value);	
		} else {
			jedis.set(key, value);
		}
	}

	@Override
	public String get(String key) {
		String value = null;
		if (null != cacheName) {
			value = jedis.hget(cacheName, key);
		} else {
			value = jedis.get(key);
		}
		return value;
	}

}
