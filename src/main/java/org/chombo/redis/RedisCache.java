/*
 * chombo: Hadoop Map Reduce utility
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.chombo.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import redis.clients.jedis.Jedis;

/**
 * @author pranab
 *
 */
public class RedisCache {
	private String cacheName;
	private Jedis jedis;
	private static final String NIL = "nil";
	
	/**
	 * Creates redis cache object for particular app
	 * @param config
	 * @param appPrefix
	 * @return
	 */
	public static RedisCache createRedisCache(Configuration config, String appPrefix) {
		RedisCache redisCache = null;
		String redisHost = config.get("redis.server.host", "localhost");
		int redisPort = config.getInt("redis.server.port",  6379);
		String defaultOrgId = config.get("default.org.id");
		if (!StringUtils.isBlank(defaultOrgId)) {
			String cacheName = appPrefix + "-" + defaultOrgId;
	   		redisCache = new   RedisCache( redisHost, redisPort, cacheName);
	   	}
		return redisCache;
	}
	
	public RedisCache(String redisHost, int redisPort, String cacheName) {
		this.cacheName = cacheName;
		jedis = new Jedis(redisHost, redisPort);
	}
	
	/**
	 * @param key
	 * @param value
	 */
	public void put(String key, String value) {
		jedis.hset(cacheName, key,  value);
	}

	/**
	 * @param keyPrefix
	 * @param value
	 * @param genKeyWithPrefix
	 */
	public void put(String keyPrefix, String value, boolean genKeyWithPrefix) {
		if (genKeyWithPrefix) {
			String key = keyPrefix + "-" + UUID.randomUUID().toString();
			jedis.hset(cacheName, key,  value);
		} else {
			jedis.hset(cacheName, keyPrefix,  value);
		}
	}

	/**
	 * @param key
	 * @return
	 */
	public String get(String key) {
		return jedis.hget(cacheName, key);
	}
	
	/**
	 * @return
	 */
	public Map<String, String>  getAll() {
		return jedis.hgetAll(cacheName);
	}
	
	/**
	 * @param keyPrefix
	 * @return
	 */
	public List<String>  getAll(String keyPrefix) {
		List<String> filtValues = new ArrayList<String>();
		Map<String, String> values =  jedis.hgetAll(cacheName);
		if (null != values) {
			for (String key :  values.keySet()) {
				if (key.startsWith(keyPrefix)) {
					filtValues.add( values.get(key).trim());
				}
			}
		}
		return filtValues;
	}

	/**
	 * @param keyPrefix
	 * @return
	 */
	public List<Integer>  getIntAll(String keyPrefix) {
		List<Integer> filtValues = new ArrayList<Integer>();
		Map<String, String> values =  jedis.hgetAll(cacheName);
		if (null != values) {
			for (String key :  values.keySet()) {
				if (key.startsWith(keyPrefix)) {
					filtValues.add(Integer.parseInt( values.get(key).trim()));
				}
			}
		}
		return filtValues;
	}

	/**
	 * Return max value among a set
	 * @param keyPrefix
	 * @return
	 */
	public int getIntMax(String keyPrefix) {
		List<Integer> values =  getIntAll(keyPrefix);
		int maxVal = Integer.MIN_VALUE;
		for (int val : values) {
			if (val > maxVal) {
				maxVal = val;
			}
		}
		return maxVal;
	}
	
	public void close() {
	}
}
 