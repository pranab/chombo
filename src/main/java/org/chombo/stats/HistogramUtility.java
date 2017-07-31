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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class HistogramUtility {
	
	/**
	 * Creates histograms from serialized data
	 * @throws IOException 
	 * 
	 */
	public static Map<String[], HistogramStat> createHiostograms(InputStream inStr, int keyLen, boolean normalized) 
			throws IOException {
		Map<String[], HistogramStat> histStats = new HashMap<String[], HistogramStat>();

		//one histogram per line of data
		List<String> lines = BasicUtils.getFileLines(inStr);
		for (String line : lines) {
			HistogramStat stat = new HistogramStat();
			String[] items = line.split(HistogramStat.fieldDelim);
			String[] key = Arrays.copyOfRange(items, 0, keyLen);
			
			stat.initialize(items, keyLen, normalized);
			histStats.put(key, stat);
		}
		
		return histStats;
	}
	
	/**
	 * @param firstStat
	 * @param secondStat
	 * @return
	 */
	public static double findKullbackLeiblerDivergence(HistogramStat firstStat, HistogramStat secondStat) {
		double divergence = 0;
		Map<Integer, Double> firstDistr = roundfOffKey(firstStat.getDistribution());
		Map<Integer, Double> secondDistr = roundfOffKey(secondStat.getDistribution());
		
		double prSum = 0;
		for (Integer key : firstDistr.keySet()) {
			Double firstVal = firstDistr.get(key);
			Double secondVal = secondDistr.get(key);
			if (null != secondVal) {
				divergence += firstVal * Math.log(firstVal /secondVal);
				prSum += firstVal;
			}
			
		}
		
		//correct for missing value in the second distr
		divergence /= prSum;
		
		return divergence;
	}

	
	/**
	 * @param distr
	 * @return
	 */
	private static Map<Integer, Double> roundfOffKey(Map<Double, Double> distr) {
		Map<Integer, Double> newDistr = new TreeMap<Integer, Double>();
		for (Double key : distr.keySet()) {
			newDistr.put((int)Math.round(key), distr.get(key));
		}
		return newDistr;
	}

}
