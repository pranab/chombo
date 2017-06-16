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
import java.util.TreeMap;

import org.chombo.util.BasicUtils;


/**
 * Histogram for categorical attributes
 * @author pranab
 *
 */
public class CategoricalHistogramStat {
	protected Map<String, Integer> binMap = new HashMap<String, Integer>();
	protected Map<String, Double> histogram = new HashMap<String, Double>();
	protected int  sampleCount;
	protected boolean extendedOutput;
	protected int outputPrecision = 3;
	private boolean debugOn = false;
	
	/**
	 * 
	 */
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
	 * @param extendedOutput
	 * @return
	 */
	public CategoricalHistogramStat withExtendedOutput(boolean extendedOutput) {
		this.extendedOutput = extendedOutput;
		return this;
	}
	
	/**
	 * @param outputPrecision
	 * @return
	 */
	public CategoricalHistogramStat withOutputPrecision(int outputPrecision) {
		this.outputPrecision = outputPrecision;
		return this;
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
	public double getGiniIndex() {
		double giniIndex = 0;
		getDistribution();
		for (String attrValue : histogram.keySet()) {
			double distrVal = histogram.get(attrValue);
			giniIndex += distrVal * distrVal;
		}
		giniIndex = 1.0 - giniIndex;
		return giniIndex;
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
	
	/**
	 * @param histStat
	 * @return
	 */
	public CategoricalHistogramStat merge(CategoricalHistogramStat histStat) {
		CategoricalHistogramStat mergedHistStat = new CategoricalHistogramStat();
		mergedHistStat.extendedOutput = extendedOutput;
		mergedHistStat.outputPrecision = outputPrecision;
		
		//bins
		for (String catAttrVal : binMap.keySet()) {
			mergedHistStat.add(catAttrVal, binMap.get(catAttrVal));
		}
		for (String catAttrVal : histStat.binMap.keySet()) {
			mergedHistStat.add(catAttrVal, histStat.binMap.get(catAttrVal));
		}
		
		return mergedHistStat;
	}

	/**
	 * returns set of items within confidence bound
	 * @param confidenceLimitPercent
	 * @return
	 */
	public List<String> getConfidenceBounds(int confidenceLimitPercent) {
		List<String> confBoundSet = new ArrayList<String>();
		int confidenceLimit = (sampleCount * confidenceLimitPercent) / 100;
		
		//sort by count
		TreeMap<Integer, String> countSortedHistogram = new TreeMap<Integer, String>();
		for(Map.Entry<String,Integer> entry : binMap.entrySet()) {
			countSortedHistogram.put(entry.getValue(), entry.getKey());
		}

		//collect high count items
		double confCount = 0;
		for(Integer count : countSortedHistogram.descendingKeySet()) {
			confCount += count;
			if (confCount < confidenceLimit) {
				confBoundSet.add(countSortedHistogram.get(count));
			}
		}
		
		return confBoundSet;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder stBld = new StringBuilder();
		final String delim = ",";
		getDistribution();
		
		//formatting
    	String formatter = "%." + outputPrecision + "f";

		//distribution
		stBld.append(histogram.size()).append(delim);
		for(String catAttrVal : histogram.keySet()) {
			double catAttrCount = histogram.get(catAttrVal);
			stBld.append(catAttrVal).append(delim).
				append(BasicUtils.formatDouble(catAttrCount, formatter)).append(delim);
		}
		
		//other stats
		if (extendedOutput) {
			String formEntropy = BasicUtils.formatDouble(getEntropy(), formatter);
			stBld.append(getMode()).append(delim).append(formEntropy).append(delim);
		}
		return stBld.substring(0, stBld.length() - 1);
	}
	
}
