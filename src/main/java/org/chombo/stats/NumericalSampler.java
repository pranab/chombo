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
 * Integer valued distribution
 * @author pranab
 *
 */
public class NumericalSampler {
	private Map<Integer, Double> distrbutions = new HashMap<Integer, Double>();
	private List<Pair<Integer, Double>> ranges = new ArrayList<Pair<Integer, Double>>();
	private double maxRange;
	private int binWidth;

	public void initialize() {
		distrbutions.clear();
		ranges.clear();
	}
	
	public NumericalSampler(int binWidth) {
		super();
		this.binWidth = binWidth;
	}

	/**
	 * @param key
	 * @param value
	 */
	public void add(Integer key, Double value) {
		distrbutions.put(key, value);
		ranges.clear();
	}
	
	/**
	 * @return
	 */
	public int sample() {
		Integer sampled = null;
		if (ranges.isEmpty()) {
			double upperBound = 0;
			for (Integer key : distrbutions.keySet()) {
				upperBound += distrbutions.get(key);
				ranges.add(new Pair<Integer, Double>(key, upperBound));
			}
			maxRange = upperBound;
		}
		
		double rand = Math.random() * maxRange;
		for (Pair<Integer, Double> item : ranges) {
			if (rand < item.getRight()) {
				sampled = item.getLeft();
			}
		}
		
		//select randomly within bin
		int randInt = (int)Math.random() * binWidth;
		sampled += randInt - binWidth / 2;
		return sampled;
	}

}
