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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * Rejection sampler with base as String, IntRange or DoubleRange
 * @author pranab
 *
 * @param <T>
 */
public class NonParametricDistrRejectionSampler<T>  implements RejectionSampler<T> {
	protected Map<T, Double> distr = new HashMap<T, Double>();
	private List<T> values;
	
	/**
	 * 
	 */
	public void intialize() {
		distr.clear();
		values = null;
	}
	
	/**
	 * @param obj
	 * @param value
	 */
	public void add(T obj, double value) {
		Double curVal = distr.get(obj);
		if (null == curVal) {
			distr.put(obj, value);
		} else {
			distr.put(obj, curVal + value);
		}
	}
	
	/**
	 * normalize
	 */
	public void normalize() {
		double sum = 0;
		for (T obj : distr.keySet()) {
			Double curVal = distr.get(obj);
			sum += curVal;
		}
		
		for (T obj : distr.keySet()) {
			Double curVal = distr.get(obj);
			distr.put(obj, curVal / sum);
		}		
	}
	
	/**
	 * @return
	 */
	public T sample() {
		T selObj = null;
		if (null == values) {
			values = new ArrayList<T>(distr.keySet());
		}
		int numValues = values.size();
		while(true) {
			//select an object randomly
			int index = (int)(Math.random() * numValues);
			T obj = values.get(index);
			
			//if a random number falls below distr then select the item and return
			if (Math.random() < distr.get(obj)) {
				selObj = obj;
				break;
			}
		}
		return selObj;
	}

}
