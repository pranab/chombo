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
 * Ramples from a random distribution
 * @author pranab
 *
 */
public class RandomSampler {
	private Map<String, Integer> distr = new HashMap<String, Integer>();
	private List<Pair<String, Integer>> distrRange = new ArrayList<Pair<String, Integer>>();
	private int max;
	
	/**
	 * 
	 */
	public void initialize() {
		distr.clear();
		distrRange.clear();
	}
	
	/**
	 * @param entity
	 * @param count
	 */
	public void addToDistr(String entity, int count) {
		distr.put(entity, count);
	}
	
	/**
	 * @return
	 */
	public String sample() {
		if (distrRange.isEmpty()) {
			createDistrRange();
		}
		int rand = (int)(Math.random() * max);
		String entity = null;
		for (Pair<String, Integer> pair :  distrRange) {
			if (rand <= pair.getRight()) {
				entity = pair.getLeft();
				break;
			}
		}
		return entity;
	}
	
	/**
	 * 
	 */
	private void createDistrRange() {
		int counter = 0;
		for (String entity : distr.keySet()) {
			counter += distr.get(entity);
			Pair<String, Integer> pair = new  Pair<String, Integer>(entity,  counter);
			distrRange.add(pair);
		}
		max = counter;
	}
}
