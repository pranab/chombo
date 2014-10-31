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

/**
 * Pub sub based on Redis queue
 * @author pranab
 *
 */
public class RedisPubSub extends PubSub{
	private MessageQueue msgQueue;
	
	/**
	 * @param conf
	 * @param storageEntity
	 * @param maxSubscriber
	 */
	public RedisPubSub(Map conf, String storageEntity, int maxSubscriber) {
		msgQueue = MessageQueue.createMessageQueue(conf, storageEntity);
		this.maxSubscriber = maxSubscriber;
	}
	
	/**
	 * @param msg
	 */
	public void publish(String msg) {
		msgQueue.send(msg);
	}
	
	/**
	 * @return
	 */
	public String pull() {
		return msgQueue.receive();
	}

}
