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

import redis.clients.jedis.Jedis;

/**
 * Redis queue
 * @author pranab
 *
 */
public class RedisQueue extends MessageQueue{
	private Jedis jedis;
	
	/**
	 * @param conf
	 * @param queue
	 */
	public RedisQueue(Map conf, String queueName) {
		String redisHost = ConfigUtility.getString(conf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(conf,"redis.server.port");
		jedis = new Jedis(redisHost, redisPort);
		this.queueName = queueName;
	}

	/* (non-Javadoc)
	 * @see org.chombo.storm.MessageQueue#send(java.lang.String)
	 */
	public void send(String msg) {
		jedis.lpush(queueName, msg);
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.storm.MessageQueue#receive()
	 */
	public String receive() {
		String msg  = jedis.rpop(queueName);
		return msg;
	}
}
