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

package org.chombo.transformer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.chombo.util.Pair;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * @author pranab
 *
 */
public abstract class JsonConverter implements Serializable {
	protected ObjectMapper mapper;
	protected Map<String, Object> map;
	protected boolean failOnInvalid;
	protected boolean normalize;
	protected static String listChild = "@a";
	protected static int  listChildLen = 2;
	protected List<String> paths;
	protected boolean debugOn;
	protected boolean autoIdGeneration;
	protected List<String> idFieldPaths;
	protected String defaultValue;
	protected boolean skipped;
	protected int skippedRecordsCount;
	protected int totalRecordsCount;
	protected int defaultValueCount;


	/**
	 * @param failOnInvalid
	 */
	public JsonConverter(boolean failOnInvalid, boolean normalize) {
		//mapper = new ObjectMapper();
		this.failOnInvalid = failOnInvalid;
		this.normalize = normalize;
	}
	
	/**
	 * @param debugOn
	 */
	public void setDebugOn(boolean debugOn) {
		this.debugOn = debugOn;
	}

	/**
	 * @return
	 */
	public JsonConverter withAutoIdGeneration() {
		this.autoIdGeneration = true;
		if (debugOn) {
			System.out.println("parent id auto generated");
		}
		return this;
	}

	/**
	 * @param idFieldPath
	 * @return
	 */
	public JsonConverter withIdFieldPaths(List<String> idFieldPaths) {
		this.idFieldPaths = idFieldPaths;
		return this;
	}
	
	/**
	 * @param defaultValue
	 * @return
	 */
	public JsonConverter withDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}

	/**
	 * @return
	 */
	public int getSkippedRecordsCount() {
		return skippedRecordsCount;
	}

	/**
	 * @return
	 */
	public int getDefaultValueCount() {
		return defaultValueCount;
	}

	/**
	 * @return
	 */
	public int getTotalRecordsCount() {
		return totalRecordsCount;
	}

	/**
	 * @param record
	 */
	protected void parse(String record) {
		try {
			if (null == mapper) {
				mapper = new ObjectMapper();
			}
			InputStream is = new ByteArrayInputStream(record.getBytes());
			map = mapper.readValue(is, new TypeReference<Map<String, Object>>() {});
		} catch (JsonParseException ex) {
			handleParseError(ex);
		} catch (JsonMappingException ex) {
			handleParseError(ex);
		} catch (IOException ex) {
			handleParseError(ex);
		}		
	}

	/**
	 * @param ex
	 */
	private void handleParseError(Exception ex) {
		map = null;
		if (failOnInvalid) {
			throw new IllegalArgumentException("failed to parse json " + ex.getMessage());
		}
	}
	
	/**
	 * @param pathElements
	 * @param index
	 */
	protected Pair<String, Integer> extractKeyAndIndex(String pathElem) {
		//extract index in case current path element point to list
		String key = null;
		int keyIndex = 0;
		
		int pos = pathElem.indexOf(listChild);
		if (pos == -1) {
			//scalar
			key = pathElem;
			if (debugOn)
				System.out.println("non array key: " + key);
		} else {
			//array
			key = pathElem.substring(0, pos);
			if (debugOn)
				System.out.println("array key: " + key);
			
			//whole list if no index provided
			String indexPart = pathElem.substring(pos + listChildLen);
			if (debugOn)
				System.out.println("indexPart: " + indexPart);
			if (!indexPart.isEmpty()) {
				keyIndex = Integer.parseInt(indexPart.substring(1));
			} else {
				keyIndex = -1;
			}
			if (debugOn)
				System.out.println("keyIndex: " + keyIndex);
		}
		
		return new Pair<String, Integer>(key, keyIndex);
	}
	
	
	/**
	 * @param record
	 * @param paths
	 * @return
	 */
	public abstract  boolean extractAllFields(String record, List<String> paths);
	
	/**
	 * @return
	 */
	public abstract  List<String[]> getExtractedRecords();
	
	/**
	 * @return
	 */
	public abstract String[] getExtractedParentRecord();
	
	/**
	 * @return
	 */
	public abstract Map<String, List<String[]>> getExtractedChildRecords();
	

}
