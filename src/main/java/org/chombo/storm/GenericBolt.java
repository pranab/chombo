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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.chombo.util.ConfigUtility;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Generic base class for all bolts
 * @author pranab
 *
 */
public abstract class GenericBolt  extends GenericComponent implements IRichBolt {
	protected List<MessageHolder> outputMessages = new ArrayList<MessageHolder>();

	private static final long serialVersionUID = 5353297284043930135L;
	protected OutputCollector collector;
	
	/**
	 * 
	 */
	public GenericBolt() {
		super();
	}
	
	/**
	 * @param fieldNames
	 */
	public GenericBolt(String...  fieldNames) {
		super(fieldNames);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		debugOn = ConfigUtility.getBoolean(stormConf, "debug.on", false);
		this.stormConf = stormConf;
		this.collector = collector;
		intialize(stormConf, context);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	public void cleanup() {
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		++messageCounter;
		if (process(input)) {
			List<MessageHolder> output = getOutput();
			if (null != output){
				if (null != streams) {
					//specific stream
					for (MessageHolder msgHolder : output) {
						String stream = msgHolder.getStream();
						collector.emit(stream, input, msgHolder.getMessage());
					}
				} else {
					//default stream
					for (MessageHolder msgHolder : output) {
						collector.emit(input, msgHolder.getMessage());
					}
				}
			}
			collector.ack(input);
		} else {
			collector.fail(input);
		}
	}


	/**
	 * Initialization for extended class
	 * @param stormConf
	 * @param context
	 */
	public abstract void intialize(Map stormConf, TopologyContext context);
	
	/**
	 * Processing of  input tuple by extended class
	 * @param input
	 * @return true if successful false otherwise
	 */
	public abstract boolean process(Tuple input);
	
	/**
	 * Extended class will return all output messages 
	 * @return output to be emitted. null if nothing to be emitted
	 */
	public abstract List<MessageHolder> getOutput();

}
