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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.chombo.util.ConfigUtility;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Values;

/**
 * Generic base class for any spout
 * @author pranab
 *
 */
public abstract class GenericSpout extends GenericComponent  implements IRichSpout {
	private static final long serialVersionUID = 5380282730606315496L;
	private boolean shouldReplayFailedMessage;
	private int maxReplayLimit;
	protected SpoutOutputCollector collector;
	private Map<String, MessageHolder> pendingMessageCache = new HashMap<String, MessageHolder>();
	private static final int MAX_REPLAY_LIMIT = 3;
	private int failedMaxReplayCount;
	private LinkedBlockingQueue<MessageHolder> messageReplayQueue;
	private long lastEmitTime;
	public static final String REPLAY_COUNT_FLD = "replayCountField";
	private static final Logger LOG = Logger.getLogger(GenericSpout.class);
	
	/**
	 * 
	 */
	public GenericSpout() {
		super();
	}
	
	/**
	 * @param fieldNames
	 */
	public GenericSpout(String...  fieldNames) {
		super(fieldNames);
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector)  {
		this.stormConf = stormConf;
		this.collector = collector;
		debugOn = ConfigUtility.getBoolean(stormConf, "debug.on", false);
		
		if (ConfigUtility.exists(stormConf, "replay.failed.message")) {
			shouldReplayFailedMessage = ConfigUtility.getBoolean(stormConf, "replay.failed.message", false);
			if (shouldReplayFailedMessage) {
				maxReplayLimit = ConfigUtility.getInt(stormConf, "max.replay.limit", MAX_REPLAY_LIMIT);
				messageReplayQueue =  new LinkedBlockingQueue<MessageHolder>();
				if (debugOn)
					LOG.info("replayFailedMessage:" + shouldReplayFailedMessage + " maxReplayLimit:" + maxReplayLimit);
			}
		}
		
		//collect all streams
		//collectStreams();

		intialize(stormConf, context);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		++messageCounter;
		MessageHolder output = null;
		//try replay queue first
		if (shouldReplayFailedMessage) {
			output = messageReplayQueue.poll();
			if (null != output) {
				output.incrReplayCount();
				
				//set replay count in field
				Values values = output.getMessage();
				values.set(values.size() -1, output.getReplayCount());
				if (debugOn)
					LOG.info("emitting msg from replay queue");
			}
		}
		
		//get from spout source if nothing from replay queue
		if (null == output) {
			output = nextSpoutMessage();
			
			if (null != output) {
				if (null == output.getMessageID()) {
					String msgID =  UUID.randomUUID().toString().replaceAll("-", "");
					output.setMessageID(msgID);
				}
				
				//initialize replay count
				if (shouldReplayFailedMessage) {
					output.getMessage().add(0);
				}
			}
		}
		
		//emit only if we have a message
		if (null != output) {
			if (null != streamFields) {
				//specific stream
				String stream = output.getStream();
				collector.emit(stream, output.getMessage(), output.getMessageID());
				if (debugOn)
					LOG.info("emitted on the stream:" + stream);
			} else {
				//default stream
				collector.emit(output.getMessage(), output.getMessageID());
			}
			pendingMessageCache.put(output.getMessageID().toString(), output);
			lastEmitTime = System.currentTimeMillis();
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
    public void ack(Object msgId) {
		pendingMessageCache.remove(msgId.toString());
    }

	/**
	 * @return
	 */
	public int pendingMessageCount() {
		return pendingMessageCache.size();
	}
	
    /* (non-Javadoc)
     * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
     */
    @Override
    public void fail(Object msgId) {
    	MessageHolder spoutMessage = pendingMessageCache.get(msgId.toString());
    	Values values = spoutMessage.getMessage();
    	handleFailedMessage(values);
    	pendingMessageCache.remove(msgId.toString());
		
		if (shouldReplayFailedMessage) {
			//add to replay queue
			if (spoutMessage.getReplayCount() < maxReplayLimit) {
				messageReplayQueue.add(spoutMessage);
				if (debugOn)
					LOG.info("put failed message in replay queue");
			} else {
				++failedMaxReplayCount;
				if (debugOn)
					LOG.info("failure count after max replay:" + failedMaxReplayCount);
			}
		}
    }
    
	/**
	 * @return
	 */
	public boolean arePendingMessages() {
		return pendingMessageCache.size() > 0;
	}
	
	/**
	 * All extended class initialization
	 * @param stormConf
	 * @param context
	 */
	public abstract void intialize(Map stormConf, TopologyContext context);

	/**
	 * Extended class should return the next message. Should return null if there is no message 
	 * @param input
	 * @return
	 */
	public abstract MessageHolder nextSpoutMessage();
	
	/**
	 * Any failure handling by the extended class 
	 * @param tuple
	 */
	public abstract void handleFailedMessage(Values tuple);
	
	/**
	 * @return
	 */
	public long getLastEmitTime() {
		return lastEmitTime;
	}

}
