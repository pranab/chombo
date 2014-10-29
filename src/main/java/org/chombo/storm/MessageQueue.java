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
 * @author pranab
 *
 */
public abstract class MessageQueue {
	protected String queueName;
	
	/**
	 * @param conf
	 * @param queueName
	 * @return
	 */
	public static MessageQueue createMessageQueue(Map conf, String queueName) {
		MessageQueue queue = null;
		String provider = ConfigUtility.getString(conf, "messaging.provider");
		
		if (provider.equals("redis")) {
			queue = new RedisQueue(conf, queueName);
		} else {
			throw new IllegalArgumentException("invalid messaging provider: " + provider);
		}
		return queue;
	}
	
	/**
	 * sends message to queue
	 * @param msg
	 */
	public abstract void send(String msg);
	
	/**
	 * receive message from queue
	 * @return
	 */
	public abstract String receive();
}
