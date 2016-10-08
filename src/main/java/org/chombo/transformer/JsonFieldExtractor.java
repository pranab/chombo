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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	private AttributeList extField = null;
	private boolean failOnInvalid;
	private boolean normalize;
	private AttributeList[] records;
	private Map<Integer, String> fieldTypes = new HashMap<Integer, String>();
	private Set<String> childObjectPaths = new HashSet<String>();
	private int numChildObjects;
	private List<String[]> extractedRecords = new ArrayList<String[]>();
	private int numAttributes;

	/**
	 * @param failOnInvalid
	 */
	public JsonFieldExtractor(boolean failOnInvalid, boolean normalize) {
		mapper = new ObjectMapper();
		this.failOnInvalid = failOnInvalid;
		this.normalize = normalize;
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
	public AttributeList extractField(String path) {
		extField = new AttributeList();
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
		int keyIndex = 0;
		String pathElem = pathElements[index];
		int pos = pathElem.indexOf("[");
		if (pos == -1) {
			//scalar
			key = pathElem;
		} else {
			//array
			key = pathElem.substring(0, pos);
			
			//whole list if no index provided
			String indexPart = pathElem.substring(pos+1);
			indexPart = indexPart.substring(0, indexPart.length()-1);
			keyIndex = !indexPart.isEmpty()? Integer.parseInt(indexPart) : -1;
		}
		
		Object obj = map.get(key);
		if (null == obj) {
			//invalid key
			if (failOnInvalid) {
				throw new IllegalArgumentException("field not reachable with json path");
			} else {
				extField.add("");
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
				if (keyIndex >= 0) {
					//specific item in list
					Object child = listObj.get(keyIndex);
					if (child instanceof Map<?,?>) {
						// non primitive list
						if (index == pathElements.length - 1) {
							throw new IllegalArgumentException("got list of map at end of json path");
						}
						
						//call recursively all map object
						extractField((Map<String, Object>)child, pathElements, index + 1);
					} else {
						//element in primitive list
						extField.add(child.toString());
					}
				} else {
					//all items in list
					if (listObj.get(0) instanceof Map<?,?>) {
						// non primitive list
						if (index == pathElements.length - 1) {
							throw new IllegalArgumentException("got list of map at end of json path");
						}
						
						//call recursively all child map object
						for (Object item : listObj) {
							extractField((Map<String, Object>)item, pathElements, index + 1);
						}
					} else {
						for (Object item : listObj) {
							//all elements in primitive list
							extField.add(item.toString());
						}						
					}
				}
			} else {
				//primitive
				extField.add(obj.toString());
			}
		}
	}	
	
	/**
	 * @param record
	 * @param paths
	 * @param items
	 * @return
	 */
	public boolean extractAllFields(String record, List<String> paths) {
		boolean valid = true;
		records = new AttributeList[paths.size()];
		parse(record);
		numChildObjects = 1;
		numAttributes = paths.size();
		if (null != map) {
			int i = 0;
			for (String path : paths) {
				AttributeList fields = extractField(path);
				if (!fields.isEmpty()) {
					records[i++] = fields;
					if (fields.size() > numChildObjects) {
						numChildObjects = fields.size();
					}
				} else {
					valid = false;
					break;
				}
			}
			
			if (normalize) {
				normalize(paths);
			} else {
				
			}
		}
		
		return valid;
	}
	
	/**
	 * @param paths
	 */
	private void normalize(List<String> paths) {
		int index = 0;
		fieldTypes.clear();
		childObjectPaths.clear();
		for (String path : paths) {
			if (isChildObject(path)) {
				String childPath = getChildPath(path);
				fieldTypes.put(index, childPath);
				childObjectPaths.add(childPath);
			} else {
				fieldTypes.put(index, "root");
			}
			++index;
		}
		
		if (childObjectPaths.size() > 1) {
			throw new IllegalStateException("can  not normalize with multiple child object types");
		}
		
		//replicated parent attributes
		replicateParentAttributes();
		
		//get normalized records
		getNormalizedRecords();
	}
	
	/**
	 * @param path
	 * @return
	 */
	private boolean isChildObject(String path) {
		//ends with either list of primitives or child of object which is an element of a list
		String[] pathElements = path.split("\\.");
		return pathElements[pathElements.length - 2].endsWith("[]") || 
				pathElements[pathElements.length - 1].endsWith("[]");
	}
	
	/**
	 * @param path
	 * @return
	 */
	private String getChildPath(String path) {
		String[] pathElements = path.split("\\.");
		String childPath = path;
		if (!pathElements[pathElements.length - 1].endsWith("[]")) {
			int pos = path.lastIndexOf(".");
			childPath =  path.substring(0, pos);
		}
		return childPath;
	}
	
	/**
	 * Replicate parent attributes
	 * 
	 */
	private void replicateParentAttributes() {
		for (AttributeList fields : records) {
			if (fields.size() == 1) {
				//parent object field
				String value = fields.get(0);
				for(int i = 1; i < numChildObjects; ++i) {
					fields.add(value);
				}
			}
		}
	}
	
	/**
	 * 
	 */
	private void getNormalizedRecords() {
		extractedRecords.clear();
		for (int i = 0; i < numChildObjects; ++i) {
			String[] record = new String[numAttributes];
			for (int j = 0; j < numAttributes; ++j) {
				record[j] = records[j].get(i);
			}
			extractedRecords.add(record);
		}
	}
	
	/**
	 * @return
	 */
	public List<String[]> getExtractedRecords() {
		return extractedRecords;
	}

	private static class AttributeList extends ArrayList<String> {}
}
