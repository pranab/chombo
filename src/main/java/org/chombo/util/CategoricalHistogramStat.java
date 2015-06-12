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

import org.chombo.util.HistogramStat.Bin;


/**
 * Histogram for categorical attributes
 * @author pranab
 *
 */
public class CategoricalHistogramStat {
	protected Map<String, Integer> binMap = new HashMap<String, Integer>();
	protected Map<String, Double> histogram = new HashMap<String, Double>();
	protected int  sampleCount;
	
	public void intialize() {
		binMap.clear();
		histogram.clear();
		sampleCount = 0;
	}
	
	/**
	 * @param value
	 */
	public void add(String value) {
		 add(value, 1);
	}	
	
	/**
	 * @param value
	 * @param count
	 */
	public void add(String value, int count) {
		Integer curAttrValCount = binMap.get(value);
		if (null == curAttrValCount) {
			curAttrValCount = count;
		} else {
			curAttrValCount += count;
		}
		binMap.put(value, curAttrValCount);
		sampleCount += count;
		histogram.clear();
	}
	
	/**
	 * @return
	 */
	public Map<String, Double> getDistribution() {
		if (histogram.isEmpty()) {
			for (String attrValue : binMap.keySet()) {
				histogram.put(attrValue,  ((double)binMap.get(attrValue)) / sampleCount);
			}
		}
		return histogram;
	}
	
	/**
	 * @return
	 */
	public double getEntropy() {
		double entropy = 0;
		getDistribution();
		for (String attrValue : histogram.keySet()) {
			double distrVal = histogram.get(attrValue);
			entropy -= distrVal * Math.log(distrVal);
		}
		return entropy;
	}

	/**
	 * @return
	 */
	public String getMode() {
		String mode = null;
		int maxCount = 0;
		for (String binIndex: binMap.keySet()) {
			int thisCount = binMap.get(binIndex);
			if (thisCount > maxCount) {
				maxCount = thisCount;
				mode = binIndex;
			}
		}		
		return mode;
	}
	
}
