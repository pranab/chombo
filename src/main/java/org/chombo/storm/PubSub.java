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
 * A pub sub system with a twist. Subscribers pull for messages. Each subscriber has 
 * unque ID which is a appended to message for reference counting purpose;
 * @author pranab
 *
 */
public abstract class PubSub {
	protected int maxSubscriber;
	protected static final String MSG_DELIM = "::";

	/**
	 * @param conf
	 * @param queueName
	 * @param maxSubscriber
	 * @return
	 */
	public static PubSub createPubSub(Map conf, String storeName, int maxSubscriber) {
		PubSub pubSub = null;
		String provider = ConfigUtility.getString(conf, "messaging.provider");
		
		if (provider.equals("redis")) {
			pubSub = new RedisPubSub(conf, storeName, maxSubscriber);
		} else {
			throw new IllegalArgumentException("invalid messaging provider: " + provider);
		}
		return pubSub;
	}
	
	/**
	 * Publish a message
	 * @param msg
	 */
	public abstract void publish(String msg);
	
	/**
	 * Pulls a message
	 * @return
	 */
	protected abstract String pull();
	
	/**
	 * Subscribe a message. Actual message is appeded with a list of subsciber ID
	 * @param clientId
	 * @return
	 */
	public String subscribe(String subscriberId) {
		String msg = null;
		String fullMsg = pull();
		if (null != fullMsg) {
			String[] items = fullMsg.split(MSG_DELIM);
			int curSubsCount = items.length - 1;
			boolean found = false;
			for (int i = 1; i < items.length; ++i) {
				if (items[i].equals(subscriberId)) {
					found = true;
					break;
				}
			}
			if (!found) {
				//this client has not received the message yet
				msg = items[0];
				
				//republish if there are pending subscribers
				if (curSubsCount + 1 <  maxSubscriber) {
					String newMsg = fullMsg + MSG_DELIM + subscriberId;
					publish(newMsg);
				}
			} else {
				//republish for other pending subscribers
				if (curSubsCount <  maxSubscriber) {
					publish(fullMsg);
				}
				
			}
		}
		
		return msg;
	}

}
