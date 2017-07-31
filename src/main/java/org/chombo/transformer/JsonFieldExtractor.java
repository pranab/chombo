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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;

/**
 * @author pranab
 *
 */
public class JsonFieldExtractor  extends JsonConverter  {
	private AttributeList extField = null;
	private AttributeList[] records;
	private Map<Integer, String> fieldTypes = new HashMap<Integer, String>();
	private Map<String, Integer> childObjectPaths = new HashMap<String, Integer>();
	private int numChildObjects;
	private List<String[]> extractedRecords = new ArrayList<String[]>();
	private int numAttributes;
	private Map<String, List<Integer>> entityColumnIndexes = new HashMap<String, List<Integer>>();
	private String[] extractedParentRecord;
	private Map<String, List<String[]>> extractedChildRecords = new HashMap<String, List<String[]>>();
	private int numParentFields;
	private static final String ROOT_ENTITY = "root";
	private String idFieldPath;
	private int idFieldIndex;

	/**
	 * @param failOnInvalid
	 */
	public JsonFieldExtractor(boolean failOnInvalid, boolean normalize) {
		super(failOnInvalid,  normalize);
	}
	
	/**
	 * @param idFieldPath
	 * @return
	 */
	@Override
	public JsonConverter withIdFieldPaths(List<String> idFieldPaths) {
		this.idFieldPath = idFieldPaths.get(0);
		if (debugOn) {
			System.out.println("parent id field defined");
		}
		return this;
	}
	

	/* (non-Javadoc)
	 * @see org.chombo.transformer.JsonConverter#getExtractedRecords()
	 */
	@Override
	public List<String[]> getExtractedRecords() {
		return extractedRecords;
	}

	/**
	 * @return
	 */
	public String[] getExtractedParentRecord() {
		return extractedParentRecord;
	}

	/**
	 * @return
	 */
	public Map<String, List<String[]>> getExtractedChildRecords() {
		return extractedChildRecords;
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
		//may have multiple column values for this path because of 1 to many parent child relationship
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
		String pathElem = pathElements[index];
		Pair<String, Integer> keyObj = extractKeyAndIndex(pathElem);
		String key = keyObj.getLeft();
		int keyIndex = keyObj.getRight();		
		
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
				if (debugOn)
					System.out.println("got map next");
				//got object
				if (index == pathElements.length - 1) {
					throw new IllegalArgumentException("got map at end of json path");
				}
				extractField((Map<String, Object>)obj, pathElements, index + 1);
			} else if (obj instanceof List<?>) { 
				if (debugOn)
					System.out.println("got list next");
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
	@Override
	public boolean extractAllFields(String record, List<String> paths) {
		boolean valid = true;
		records = new AttributeList[paths.size()];
		parse(record);
		numChildObjects = 1;
		numAttributes = paths.size();
		if (null != map) {
			int i = 0;
			
			//each path corresponds to a column in flattened data
			for (String path : paths) {
				if (debugOn)
					System.out.println("next path: " + path);
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
			
			//post process
			if (!normalize) {
				deNormalize(paths);
			} else {
				normalize(paths);
			}
		}
		
		return valid;
	}
	
	/**
	 * @param paths
	 */
	private void deNormalize(List<String> paths) {
		int index = 0;
		fieldTypes.clear();
		childObjectPaths.clear();
		
		//all paths
		for (String path : paths) {
			//one to many child
			if (isChildObject(path)) {
				String childPath = getChildPath(path);
				fieldTypes.put(index, childPath);
				
				//number of child object fields for a given child path
				childObjectPaths.put(childPath, records[index].size());
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
		getDeNormalizedRecords();
	}
	
	/**
	 * @param path
	 * @return
	 */
	private boolean isChildObject(String path) {
		//ends with either list of primitives or child of object which is an element of a list
		String[] pathElements = path.split("\\.");
		int len = pathElements.length;
		return len >= 2 && pathElements[len - 2].endsWith(listChild) || 
				pathElements[len - 1].endsWith(listChild);
	}
	
	/**
	 * @param path
	 * @return
	 */
	private String getChildPath(String path) {
		String[] pathElements = path.split("\\.");
		String childPath = path;
		if (!pathElements[pathElements.length - 1].endsWith(listChild)) {
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
				
				//replicate as many times as the number of child objects
				for(int i = 1; i < numChildObjects; ++i) {
					fields.add(value);
				}
			}
		}
	}
	
	/**
	 * 
	 */
	private void getDeNormalizedRecords() {
		extractedRecords.clear();
		
		//rows 
		for (int i = 0; i < numChildObjects; ++i) {
			String[] record = new String[numAttributes];
			
			//fields
			for (int j = 0; j < numAttributes; ++j) {
				record[j] = records[j].get(i);
			}
			extractedRecords.add(record);
		}
	}
	

	/**
	 * @param paths
	 */
	private void normalize(List<String> paths) {
		int index = 0;
		childObjectPaths.clear();
		numParentFields = 0;
		entityColumnIndexes.clear();
		idFieldIndex = -1;
		for (String path : paths) {
			if (isChildObject(path)) {
				//child
				String childPath = getChildPath(path);
				
				//field indexes for this child object
				List<Integer> indexes = getEnityColIndexes(childPath);
				indexes.add(index);
				childObjectPaths.put(childPath, records[index].size());
			} else {
				//root
				List<Integer> indexes = getEnityColIndexes(ROOT_ENTITY);
				indexes.add(index);
				
				//root object ID field
				if (null != idFieldPath && path.equals(idFieldPath)) {
					idFieldIndex = index;
				}
			}
			++index;
		}
		
		//auto generate Id
		if (-1 == idFieldIndex) {
			if (autoIdGeneration) {
				idFieldIndex = 0;
			}
		}
		
		if (idFieldIndex < 0) {
			throw new IllegalStateException("parent entity id field not found");
		}
		
		//build parent record
		buildParentRecord();
		
		//build all child records
		buildChildRecords();
	}	
	
	/**
	 * @param entity
	 * @return
	 */
	private List<Integer> getEnityColIndexes(String entity) {
		List<Integer> indexes = entityColumnIndexes.get(entity);
		if (null == indexes) {
			indexes = new ArrayList<Integer>();
			entityColumnIndexes.put(entity, indexes);
		}
		return indexes;
	}
	
	/**
	 * 
	 */
	private void buildParentRecord() {
		List<Integer> indexes = getEnityColIndexes(ROOT_ENTITY);
		int i = 0;
		if (autoIdGeneration) {
			//additional field for entity type synthetic Id
			extractedParentRecord = new String[indexes.size() + 2];
			extractedParentRecord[i++] = ROOT_ENTITY;
			extractedParentRecord[i++] = BasicUtils.generateId();
		} else {
			//additional field for entity type
			extractedParentRecord = new String[indexes.size() + 1];
			extractedParentRecord[i++] = ROOT_ENTITY;
		}
		
		//populate all fields of root object
		for (int index : indexes) {
			extractedParentRecord[i++] = records[index].get(0);
		}
	}
	
	/**
	 * 
	 */
	private void buildChildRecords() {
		extractedChildRecords.clear();
		
		//all child objects
		for (String entity : entityColumnIndexes.keySet()) {
			if (!entity.equals(ROOT_ENTITY)) {
				List<String[]> childRecList = new ArrayList<String[]>();
				
				//field indexes for this child object
				List<Integer> indexes = getEnityColIndexes(entity);
				
				//number of records for this child object
				int numRecs = childObjectPaths.get(entity);
				
				//for all child records
				for(int i = 0; i < numRecs; ++i) {
					//additional fields for entity type and parent ID
					String[] childRec = new String[indexes.size() + 2];
					int j = 0;
					
					//entity type 
					childRec[j++] = entity;
					
					//and reference to parent record, index shifted to accommodate entity type in parent record
					childRec[j++] = extractedParentRecord[idFieldIndex + 1];
					
					//all child record fields
					for (int index : indexes) {
						childRec[j++] = records[index].get(i);
					}
					childRecList.add(childRec);
				}
				extractedChildRecords.put(entity, childRecList);
			}
		}
	}
	
	private static class AttributeList extends ArrayList<String> {}
}
