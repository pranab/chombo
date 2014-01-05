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

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

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
	
	/**
	 * 
	 */
	public GenericComponent() {
	}
	
	/**
	 * @param fieldNames
	 */
	public GenericComponent(String...  fieldNames) {
		this.fieldNames = fieldNames;
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
			if (null != fieldNames) {
				fieldsObj = new Fields(Arrays.asList(fieldNames));
				declarer.declare(fieldsObj);
			}
		} else {
			//stream specific field declaration
			for (String stream : streamFields.keySet()) {
				fieldsObj = new Fields(Arrays.asList(streamFields.get(stream)));
				declarer.declareStream(stream, fieldsObj);
			}
		}
	}

}
