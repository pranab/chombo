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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Removes duplicates. Order may be preserved in collection generated
 * @author pranab
 *
 * @param <T>
 */
public class DuplicateRemover<T> {
	private boolean preserveOrder;
	private Collection<T> collection;

	/**
	 * 
	 */
	public DuplicateRemover() {
		this(false);
	}
	
	/**
	 * @param preserveOrder
	 */
	public DuplicateRemover(boolean preserveOrder) {
		if (preserveOrder) {
			collection = new ArrayList<T>();
		} else {
			collection = new HashSet<T>();
		}
		this.preserveOrder = preserveOrder;
	}
	
	/**
	 * 
	 */
	public void initialize() {
		collection.clear();
	}
	
	/**
	 * @param obj
	 */
	public void add(T obj) {
		if (preserveOrder) {
			if (!collection.contains(obj)) {
				collection.add(obj);
			}
		} else {
			collection.add(obj);
		}
	}
	
	/**
	 * @param objs
	 */
	public void add(List<T> objs) {
		for (T obj : objs) {
			add(obj);
		}
	}
	
	/**
	 * @return
	 */
	public Collection<T> getData() {
		return collection;
	}
	
	/**
	 * @return
	 */
	public int getSize() {
		return collection.size();
	}
}
