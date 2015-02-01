/*
 * chombo: Hadoop Map Reduce and Storm utility
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

package org.chombo.storm;

import java.util.Map;

import org.chombo.util.ConfigUtility;

/**
 * Abstract calss for simple cache interface
 * @author pranab
 *
 */
public abstract class Cache {
	protected String cacheName;
	
	public abstract void set(String key, String value);
	
	public abstract String get(String key);

	public static Cache createCache(Map conf, String cacheName) {
		Cache cache = null;
		String provider = ConfigUtility.getString(conf, "messaging.provider");
		if (provider.equals("redis")) {
			cache = new RedisCache(conf, cacheName);
		} else {
			throw new IllegalArgumentException("invalid messaging provider: " + provider);
		}
		
		return cache;
	}
	
	public static Cache createCache(Map conf) {
		return createCache(conf, null);
	}	
}
