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

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

/**
 * @author pranab
 *
 */
public class RedisCache {
	private String cacheName;
	private Jedis jedis;
	private static final String NIL = "nil";
	
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
	public Map<String, String>  getAll(String keyPrefix) {
		Map<String, String> filtValues = new HashMap<String, String>();
		Map<String, String> values =  jedis.hgetAll(cacheName);
		for (String key :  values.keySet()) {
			if (key.startsWith(keyPrefix)) {
				filtValues.put(key, values.get(key));
			}
		}
		
		return filtValues;
	}

}
 