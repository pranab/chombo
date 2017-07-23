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

package org.chombo.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 * @param <T>
 */
public class ValueCounters<T> {
	private Map<T, ValueCounter<T>> counters = new HashMap<T, ValueCounter<T>>();
	private boolean ascending = true;
	
	/**
	 * @param ascending
	 */
	public ValueCounters(boolean ascending) {
		this.ascending = ascending;
	}
	
	/**
	 * @return
	 */
	public Map<T, ValueCounter<T>> getCounters() {
		return counters;
	}

	/**
	 * @param obj
	 * @param count
	 */
	public void add(T obj, int count) {
		ValueCounter<T> counter = counters.get(obj);
		if (null == counter) {
			counter = new ValueCounter<T>(obj,  count, ascending);
			counters.put(obj, counter);
		} else {
			counter.add(count);
		}
	}
	
	/**
	 * @return
	 */
	public List<ValueCounter<T>> getSorted() {
		List<ValueCounter<T>> counterList = new ArrayList<ValueCounter<T>>();
		for (T obj : counters.keySet()) {
			counterList.add(counters.get(obj));
		}
		Collections.sort(counterList);
		return counterList;
	}
}
