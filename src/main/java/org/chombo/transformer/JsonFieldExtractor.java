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
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * @author pranab
 *
 */
public class JsonFieldExtractor {
	private ObjectMapper mapper;
	private Map<String, Object> map;
	private String extField = null;
	private boolean failOnInvalid;

	/**
	 * @param failOnInvalid
	 */
	public JsonFieldExtractor(boolean failOnInvalid) {
		mapper = new ObjectMapper();
		this.failOnInvalid = failOnInvalid;
	}
	
	/**
	 * @param record
	 */
	public void parse(String record) {
		try {
			InputStream is = new ByteArrayInputStream((record).getBytes());
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
	 * @param path
	 * @return
	 */
	public String extractField(String path) {
		extField = null;
		if (null != map) {
			String[] pathElements = path.split("\\.");
			extractField(map, pathElements, 0);
		}
		return extField;
	}

	/**
	 * @param map
	 * @param pathElements
	 * @param index
	 */
	public void extractField(Map<String, Object> map, String[] pathElements, int index) {
		//extract index in case current path element point to list
		String key = null;
		int KeyIndex = 0;
		String pathElem = pathElements[index];
		int pos = pathElem.indexOf("[");
		if (pos != -1) {
			key = pathElem;
		} else {
			key = pathElem.substring(0, pos);
			String indexPart = pathElem.substring(pos+1);
			KeyIndex = Integer.parseInt(indexPart.substring(0, indexPart.length()-1));
		}
		
		Object obj = map.get(pathElements[index]);
		if (null == obj) {
			//invalid key
			if (failOnInvalid) {
				throw new IllegalArgumentException("field not reachable with json path");
			} else {
				extField = null;
			}
		} else {
			//traverse further
			if (obj instanceof Map<?,?>) {
				//got object
				if (index == pathElements.length - 1) {
					throw new IllegalArgumentException("got map at end of json path");
				}
				extractField((Map<String, Object>)obj, pathElements, index + 1);
			} else if (obj instanceof List<?>) { 
				//got list
				List<?> listObj = (List<?>)obj;
				Object child = listObj.get(KeyIndex);
				if (child instanceof Map<?,?>) {
					// non primitive list
					if (index == pathElements.length - 1) {
						throw new IllegalArgumentException("got list of map at end of json path");
					}
					extractField((Map<String, Object>)child, pathElements, index + 1);
				} else {
					//primitive list
					extField = child.toString();
				}
				
			} else {
				//primitive
				extField = obj.toString();
			}
		}
	}	
	
	/**
	 * @param record
	 * @param paths
	 * @param items
	 * @return
	 */
	public boolean extractAllFields(String record, List<String> paths, String[] items) {
		boolean valid = true;
		parse(record);
		if (null != map) {
			int i = 0;
			for (String path : paths) {
				String field = extractField(path);
				if (null != field) {
					items[i++] = field;
				} else {
					valid = false;
					break;
				}
			}
		}
		
		return valid;
	}
	
}
