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

package org.chombo.storm;

import java.io.Serializable;

import backtype.storm.tuple.Values;

/**
 * @author pranab
 * Holds message and related message meta data
 */
public class MessageHolder implements Serializable {
	private Values message;
	private Object messageID;
	private String stream;
	private int replayCount;
	private static final long serialVersionUID = 8238179629177882915L;

	public MessageHolder() {
	}

	public MessageHolder(Values message, Object messageID) {
		this.message = message;
		this.messageID = messageID;
	}

	public MessageHolder(Values message) {
		this.message = message;
	}

	public Values getMessage() {
		return message;
	}

	public void setMessage(Values message) {
		this.message = message;
	}

	public Object getMessageID() {
		return messageID;
	}

	public void setMessageID(Object messageID) {
		this.messageID = messageID;
	}
	
	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void incrReplayCount() {
		++replayCount;
	}

	public int getReplayCount() {
		return replayCount;
	}

}
