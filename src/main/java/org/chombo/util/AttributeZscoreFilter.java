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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.chombo.stats.NumericalAttrStatsManager;

/**
 * Zscore based outlier detector 
 * @author pranab
 *
 */
public class AttributeZscoreFilter {
	private Map<Integer, ZscoreFilter> filters = new HashMap<Integer, ZscoreFilter>();

	/**
	 * @param attrZscores
	 * @param config
	 * @param statsFilePathParam
	 * @param fieldDelim
	 * @throws IOException
	 */
	public AttributeZscoreFilter(Map<Integer, Double> attrZscores, Configuration config, String statsFilePathParam, 
			String fieldDelim) throws IOException {
		NumericalAttrStatsManager statsManager = new NumericalAttrStatsManager(config, statsFilePathParam, fieldDelim); 
		for (int attr : attrZscores.keySet()) {
			double zscore = attrZscores.get(attr);
			double mean = statsManager.getMean(attr);
			double stdDev = statsManager.getStdDev(attr);
			filters.put(attr, new ZscoreFilter(mean, stdDev, zscore));
		}
	}
	
	/**
	 * @param attrOrd
	 * @param value
	 * @return
	 */
	public boolean isWithinBound(int attrOrd, double value) {
		return filters.get(attrOrd).isWithinBound(value);
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class ZscoreFilter {
		private double lowerBound;
		private double upperBound;
		
		public ZscoreFilter(double mean, double stdDev, double zscore) {
			lowerBound = mean - zscore * stdDev;
			upperBound = mean + zscore * stdDev;
		}
		
		public boolean isWithinBound(double value) {
			return value >= lowerBound && value <= upperBound;
		}
	}
}
