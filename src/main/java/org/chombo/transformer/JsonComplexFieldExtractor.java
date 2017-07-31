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

import java.io.Serializable;
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
public class JsonComplexFieldExtractor extends JsonConverter {
	private DataFlatenerNode flattenerRoot = new DataFlatenerNode("$root");
	
	/**
	 * @param failOnInvalid
	 * @param normalize
	 */
	public JsonComplexFieldExtractor(boolean failOnInvalid, boolean normalize){
		super(failOnInvalid, normalize);
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.transformer.JsonConverter#extractAllFields(java.lang.String, java.util.List)
	 */
	@Override
	public boolean extractAllFields(String record, List<String> paths) {
		this.paths = paths;
		
		//initialize
		initialize();
		
		//parse json
		parse(record);
		
		//extract fields
		if (null != map) {
			for (String path : paths) {
				extractfield(path);
			}
		}
		
		//collect data
		if (normalize) {
			//TODO
			
		} else {
			collectData(flattenerRoot, null);
		}
		
		return true;
	}
	
	/**
	 * 
	 */
	private void initialize() {
		//clear all data
		clearData(flattenerRoot);
	}
	
	/**
	 * @param path
	 */
	private void extractfield(String path){
		String[] pathElements = path.split("\\.");
		extractField(map, pathElements, 0, flattenerRoot);
	}
	
	/**
	 * @param map
	 * @param pathElements
	 * @param index
	 * @param flattener
	 */
	public void extractField(Map<String, Object> map, String[] pathElements, int index, DataFlatenerNode flattener) {
		String pathElem = pathElements[index];
		Pair<String, Integer> keyObj = extractKeyAndIndex(pathElem);
		String key = keyObj.getLeft();
		int keyIndex = keyObj.getRight();
		String fullKey = BasicUtils.join(pathElements, 0, index+1, ".");
		
		
		Object obj = map.get(key);
		if (null == obj) {
			//invalid key
			if (failOnInvalid) {
				throw new IllegalArgumentException("field not reachable with json path key:" + key);
			} else {
				//TODO
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
				DataFlatenerNode childFlattener = flattener.getChild(fullKey);
				if (debugOn)
					System.out.println("flattener for obj fullKey: " + fullKey + " flattener " + childFlattener);
				extractField((Map<String, Object>)obj, pathElements, index + 1, childFlattener);
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
						DataFlatenerNode childFlattener = flattener.getChild(fullKey, keyIndex, listObj.size());
						extractField((Map<String, Object>)child, pathElements, index + 1, childFlattener);
					} else {
						//elements in primitive list
						flattener.setData(fullKey, child.toString());
					}
				} else {
					//all items in list
					if (listObj.get(0) instanceof Map<?,?>) {
						// non primitive list
						if (index == pathElements.length - 1) {
							throw new IllegalArgumentException("got list of map at end of json path");
						}
						
						//call recursively all child map object
						int i = 0;
						for (Object item : listObj) {
							DataFlatenerNode childFlattener = flattener.getChild(fullKey, i, listObj.size());
							if (debugOn)
								System.out.println("flattener obj in array fullKey: " + fullKey + " index:" + i + " list size:" + listObj.size());
							extractField((Map<String, Object>)item, pathElements, index + 1, childFlattener);
							++i;
						}
					} else {
						for (Object item : listObj) {
							//all elements in primitive list
							flattener.setData(fullKey, item.toString());
						}						
					}
				}
			} else {
				//primitive
				flattener.setData(fullKey, obj.toString());
			}
		}
	}

	/**
	 * @param node
	 * @param parent
	 */
	private void clearData(DataFlatenerNode node) {
		//depth first traversal
		Map<String, DataFlatenerNode[]> children =  node.getChildren();
		for (String key : children.keySet()) {
			for (DataFlatenerNode child :  children.get(key)) {
				clearData(child);
			}
		}
		
		//clear data
		node.getData().clear();
	}
	
	/**
	 * @param node
	 * @param parent
	 */
	private void collectData(DataFlatenerNode node, DataFlatenerNode parent) {
		//depth first traversal
		Map<String, DataFlatenerNode[]> children =  node.getChildren();
		for (String key : children.keySet()) {
			for (DataFlatenerNode child :  children.get(key)) {
				collectData(child, node);
			}
		}
		
		//replicate parent data due to de normalization
		node.replicate();
		
		//propagate child data to parent
		if (null != parent) {
			Map<String, List<String>> childData = node.getData();
			parent.addData(childData);
		}
	}

	/* (non-Javadoc)
	 * @see org.chombo.transformer.JsonConverter#getExtractedRecords()
	 */
	@Override
	public List<String[]> getExtractedRecords() {
		List<String[]> table = new ArrayList<String[]>();
		
		if (normalize) {
			//TODO
			
		} else {
			int size = -1;
			List<List<String>> columns = new ArrayList<List<String>>();
			for (String path : paths) {
				List<String> column = flattenerRoot.getData(path);
				columns.add(column);
				if (size < 0) {
					size = column.size();
				} else if (column.size() != size) {
					throw new IllegalStateException("invalid denormalization, unequal column size");
				}
			}
			
			//create row and add to table
			int numRow = size;
			int numCol = paths.size();
			if(debugOn)
					System.out.println("numRow:" + numRow + " numCol:" + numCol);
			for (int i = 0; i < numRow; ++i) {
				String[] row = new String[numCol];
				for (int j = 0; j < numCol; ++j) {
					row[j] = columns.get(j).get(i);
				}
				table.add(row);
			}
		}
		return table;
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class DataFlatenerNode implements Serializable {
		private Map <String, DataFlatenerNode[]> children = new HashMap <String, DataFlatenerNode[]>();
		private DataFlatenerNode parent;
		private String key;
		private Map<String, List<String>> data = new HashMap<String, List<String>>();
		
		/**
		 * @param key
		 */
		public DataFlatenerNode(String key) {
			this.key = key;
		}
		
		/**
		 * @param key
		 * @param parent
		 */
		public DataFlatenerNode(String key, DataFlatenerNode parent) {
			this.key = key;
			this.parent = parent;
		}

		/**
		 * @return
		 */
		public Map<String, DataFlatenerNode[]> getChildren() {
			return children;
		}

		/**
		 * @return
		 */
		public Map<String, List<String>> getData() {
			return data;
		}

		/**
		 * @param key
		 * @return
		 */
		public List<String> getData(String key) {
			return data.get(key);
		}

		/**
		 * @param key
		 * @return
		 */
		public DataFlatenerNode getChild(String key) {
			DataFlatenerNode child = null;
			DataFlatenerNode[] theseChildren = children.get(key);
			if (null == theseChildren) {
				theseChildren = new DataFlatenerNode[1];
				children.put(key, theseChildren);
				child = new DataFlatenerNode(key, this);
				theseChildren[0] = child;
			} else {
				child = theseChildren[0];
			}
			
			if (null == child) {
				System.out.println("** got null flattener");
			}
			return child;
		}
		
		/**
		 * @param key
		 * @param childIndex
		 * @param numChildren
		 * @return
		 */
		public DataFlatenerNode getChild(String key, int childIndex, int numChildren) {
			DataFlatenerNode child = null;
			DataFlatenerNode[] theseChildren = children.get(key);
			
			//invalidate array
			if (null != theseChildren && theseChildren.length != numChildren) {
				theseChildren = null;
			}
			
			//create child array 
			if (null == theseChildren) {
				theseChildren = new DataFlatenerNode[numChildren];
				for (int i = 0; i < numChildren; ++i) {
					theseChildren[i] = null;
				}
				children.put(key, theseChildren);
			} 
			
			child = theseChildren[childIndex];
			if (null == child) {
				child = new DataFlatenerNode(key, this);
				theseChildren[childIndex] = child;
			}
			
			//if (null == child) {
			//	System.out.println("** got null flattener");
			//}
			return child;
		}
		
		/**
		 * @param key
		 * @param value
		 */
		public void setData(String key, String value) {
			List<String> values = data.get(key);
			if (null == values) {
				values = new ArrayList<String>();
				data.put(key, values);
			}
			values.add(value);
		}

		
		/**
		 * replicate parent data
		 * 
		 */
		public void replicate() {
			//child data size
			int maxSize = 0;
			for (String key : data.keySet()) {
				List<String> values = data.get(key);
				if (values.size() > 1) {
					maxSize = values.size();
					break;
				}
			}

			//replicate for parent
			if (maxSize > 1) {
				for (String key : data.keySet()) {
					List<String> values = data.get(key);
					if (values.size() == 1) {
						String value = values.get(0);
						for (int i = 1; i < maxSize; ++i) {
							values.add(value);
						}
					}
				}
			}
		}
		
		/**
		 * propogate child data to parent
		 * @param childData
		 */
		public void addData(Map<String, List<String>> childData) {
			for (String key : childData.keySet()) {
				List<String> childValues = childData.get(key);
				List<String> values = data.get(key);
				if (null == values) {
					data.put(key, childValues);
				} else {
					values.addAll(childValues);
				}
			}			
		}
	}

	@Override
	public String[] getExtractedParentRecord() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<String[]>> getExtractedChildRecords() {
		// TODO Auto-generated method stub
		return null;
	}

}
