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


package org.chombo.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic tree
 * @author pranab
 *
 * @param <T>
 */
public   class NamedObjectTree<T extends NamedObject> {
	protected T current;
	protected Map<String, NamedObjectTree<T>> children = new HashMap<String, NamedObjectTree<T>>();
	
	/**
	 * @param current
	 */
	public NamedObjectTree(T current) {
		super();
		this.current = current;
	}

	/**
	 * Adds child node
	 * @param child
	 */
	public void addChild(T child) {
		NamedObjectTree<T>  childNode = new  NamedObjectTree<T>(child);
		children.put(child.getName(), childNode);
	}
	
	/**
	 * Adds child node
	 * @param child
	 */
	private  void addChild(NamedObjectTree<T> childNode) {
		children.put(childNode.current.getName(), childNode);
	}

	/**
	 * @param path between current node and parent of node to be added
	 * @param child
	 */
	public void addChild(String[] path, T child) {
		NamedObjectTree<T>  thisNode  = this;
		for (String childName : path) {
			thisNode = thisNode.children.get(childName);
			if (null == thisNode) {
				throw new IllegalArgumentException("invalid parent path");
			}
		}
		thisNode.addChild(child);
	}
	
	/**
	 * Gets immediate child
	 * @param childName
	 * @return
	 */
	public T  getChild(String childName) {
		return children.get(childName).current;
	}
	
	/**
	 * Gets a node at the end of the path. Created node along the way if needed
	 * @param path between current node and node desired
	 * @return
	 */
	public T  getDescendent(String[] path) {
		return getDescendent(path,false);
	}

	/**
	 * Gets a node at the end of the path. Created node along the way if needed
	 * @param path between current node and node desired
	 * @return
	 */
	public T  getOrAddDescendent(String[] path) {
		return getDescendent(path,true);
	}	
	
	/**
	 * Gets a node at the end of the path. Created node along the way if needed
	 * @param path between current node and node desired
	 * @return
	 */
	public T  getDescendent(String[] path, boolean create) {
		NamedObjectTree<T> childNode = this;
		NamedObjectTree<T> nextChildNode = null;
		for (String childName : path) {
			nextChildNode = childNode.children.get(childName);
			if (null == nextChildNode) {
				if (create) {
					//alowed to create
					T child = createChild(childName);
					if (null == child) {
						throw new IllegalStateException("createChild method must be overridden");
					}
					nextChildNode  = new   NamedObjectTree<T>(child);
					childNode.addChild(nextChildNode);
				} else {
					//supposed to exist
					throw new IllegalArgumentException("failed to find node along path");
				}
			}
			childNode = nextChildNode;
		}
		return childNode.current;
	}

	protected T createChild(String childName) {
		return null;
	}
	
	/**
	 * @return
	 */
	public T getCurrent() {
		return current;
	}

	/**
	 * @param current
	 */
	public void setCurrent(T current) {
		this.current = current;
	}

	/**
	 * @return
	 */
	public Map<String, NamedObjectTree<T>> getChildren() {
		return children;
	}

	public void setChildren(Map<String, NamedObjectTree<T>> children) {
		this.children = children;
	}
	
}
