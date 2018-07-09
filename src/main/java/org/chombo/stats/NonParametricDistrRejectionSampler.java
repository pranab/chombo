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
	protected Map<T, Double> normDistr = new HashMap<T, Double>();
	protected List<T> values;
	protected boolean normalized;
	
	/**
	 * 
	 */
	public void initialize() {
		distr.clear();
		values = null;
		normalized = false;
	}
	
	/**
	 * @param that
	 */
	public void merge(NonParametricDistrRejectionSampler<T> that) {
		for (T key : that.distr.keySet()) {
			Double thatVal = that.distr.get(key);
			Double thisVal = this.distr.get(key);
			if (null == thisVal) {
				distr.put(key, thatVal);
			} else {
				distr.put(key, thisVal + thatVal);
			}
			normalized = false;
		}
	}

	/**
	 * @param obj
	 * @param value
	 */
	public void add(T obj) {
		add(obj, 1);
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
		normalized = false;
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
		
		normDistr.clear();
		for (T obj : distr.keySet()) {
			Double curVal = distr.get(obj);
			normDistr.put(obj, curVal / sum);
		}		
		normalized = true;
	}
	
	/**
	 * @return
	 */
	public T sample() {
		if (!normalized) {
			normalize();
		}
		T selObj = null;
		if (null == values) {
			values = new ArrayList<T>(normDistr.keySet());
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

	/**
	 * @return
	 */
	public Map<T, Double> getDistr() {
		return distr;
	}
	
	/**
	 * @return
	 */
	public Map<T, Double> getNormDistr() {
		if (!normalized) {
			normalize();
		}
		return normDistr;
	}
	
	/**
	 * @return
	 */
	public T getMode() {
		T mode = null;
		if (!normalized) {
			normalize();
		}
		double dist = -1;
		for (T key : normDistr.keySet()) {
			if (normDistr.get(key) > dist) {
				dist = normDistr.get(key);
				mode = key;
			}
		}
		return mode;
	}

}
