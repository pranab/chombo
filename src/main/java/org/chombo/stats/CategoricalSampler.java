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

import org.chombo.util.Pair;

/**
 * String valued distribution
 * @author pranab
 *
 */
public class CategoricalSampler {
	private Map<String, Double> distrbutions = new HashMap<String, Double>();
	private List<Pair<String, Double>> ranges = new ArrayList<Pair<String, Double>>();
	private double maxRange;

	public void initialize() {
		distrbutions.clear();
		ranges.clear();
	}

	/**
	 * @param key
	 * @param value
	 */
	public void add(String key, Double value) {
		distrbutions.put(key, value);
		ranges.clear();
	}
	
	/**
	 * @param key
	 * @return
	 */
	public double get(String key) {
		return distrbutions.get(key);
	}
	
	/**
	 * @param key
	 * @param value
	 */
	public void set(String key, Double value) {
		distrbutions.put(key, value);
		ranges.clear();
	}

	/**
	 * @return
	 */
	public String sample() {
		String sampled = null;
		if (ranges.isEmpty()) {
			double upperBound = 0;
			for (String key : distrbutions.keySet()) {
				upperBound += distrbutions.get(key);
				ranges.add(new Pair<String, Double>(key, upperBound));
			}
			maxRange = upperBound;
		}
		
		double rand = Math.random() * maxRange;
		for (Pair<String, Double> item : ranges) {
			if (rand < item.getRight()) {
				sampled = item.getLeft();
			}
		}
		
		return sampled;
	}
	
	
}
