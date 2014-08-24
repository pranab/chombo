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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import backtype.storm.Constants;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Mother of all base classes
 * @author pranab
 *
 */
public class GenericComponent {
	protected  String[] fieldNames;
	protected  Map<String, String[]> streamFields;
	protected 	String[] streams;
	protected Map stormConf;
	protected  boolean debugOn;
	protected long messageCounter;
	protected int messageCountInterval;
	protected String ID;
	private static final Logger LOG = Logger.getLogger(GenericComponent.class);
	
	/**
	 * 
	 */
	public GenericComponent() {
		ID = UUID.randomUUID().toString().replaceAll("-", "");
	}
	
	/**
	 * @param fieldNames
	 */
	public GenericComponent(String...  fieldNames) {
		this.fieldNames = fieldNames;
	}

	public String getID() {
		return ID;
	}

	/**
	 * @param fieldNames
	 * @return
	 */
	public GenericComponent withTupleFields(String...  fieldNames) {
		this.fieldNames = fieldNames;
		return this;
	}
	/**
	 * stream specific field declaration
	 * @param fieldNames
	 * @return
	 */
	public GenericComponent withStreamTupleFields(String stream, String...  fieldNames) {
		if (null == streamFields) {
			streamFields = new HashMap<String, String[]>();
		}
		streamFields.put(stream, fieldNames);
		return this;
	}

	/**
	 * 
	 */
	protected void collectStreams() {
		//collect all streams
		if (null != streamFields) {
			if(debugOn)
				LOG.info("number of streams:" +streamFields.size());
			streams = new String[streamFields.size()];
			int i = 0;
			for (String stream : streamFields.keySet()) {
				streams[i++] = stream;
			}
		}
	}

	/**
	 * @param declarer
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fieldsObj = null;
		if (null == streamFields) {
			//default field declaration
			if (debugOn)
				LOG.info("declaring output fields for default stream");
			if (null != fieldNames) {
				fieldsObj = new Fields(Arrays.asList(fieldNames));
				declarer.declare(fieldsObj);
			}
		} else {
			//stream specific field declaration
			for (String stream : streamFields.keySet()) {
				if (debugOn)
					LOG.info("declaring output fields for specified  stream:" + stream);
				fieldsObj = new Fields(Arrays.asList(streamFields.get(stream)));
				declarer.declareStream(stream, fieldsObj);
			}
		}
	}

	protected  boolean isTickTuple(Tuple tuple) {
		  return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
		    && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
}
