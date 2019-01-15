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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.chombo.util.BasicUtils;
import org.chombo.util.IntRange;
import org.chombo.util.Pair;

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
			if (line.startsWith("(")) {
				line = line.substring(1, line.length()-1);
			}
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
	public static Pair<Double, Integer> findKullbackLeiblerDivergence(HistogramStat firstStat, HistogramStat secondStat) {
		double divergence = 0;
		Map<Integer, Double> firstDistr = roundfOffKey(firstStat.getDistribution());
		Map<Integer, Double> secondDistr = roundfOffKey(secondStat.getDistribution());
		
		double prSum = 0;
		int keyMatched = 0;
		for (Integer key : firstDistr.keySet()) {
			Double firstVal = firstDistr.get(key);
			Double secondVal = secondDistr.get(key);
			if (null != secondVal) {
				divergence += firstVal * Math.log(firstVal /secondVal);
				prSum += firstVal;
				++keyMatched;
			}
		}
		
		Pair<Double, Integer> result = new Pair<Double, Integer>();
		if (keyMatched < firstDistr.size() / 2) {
			//not enough overlap between the 2 distributions
			result.setLeft(0.0);
			result.setRight(keyMatched);
		} else {
			//calculate KL divergence
			divergence /= prSum;
			result.setLeft(divergence);
			result.setRight(keyMatched);
		}
		
		return result;
	}

	
	/**
	 * @param distr
	 * @return
	 */
	private static Map<Integer, Double> roundfOffKey(Map<Double, Double> distr) {
		//convert keys to integer and sort by key
		Map<Integer, Double> newDistr = new TreeMap<Integer, Double>();
		for (Double key : distr.keySet()) {
			int iKey = scaleAndRound(key, 100);
			newDistr.put(iKey, distr.get(key));
		}
		return newDistr;
	}
	
	/**
	 * @param value
	 * @param min
	 * @return
	 */
	private static int scaleAndRound(double value, int min) {
		int k = 1;
		int iValue = (int)Math.round((value * k));
		while (iValue < min) {
			k *= 10;
			iValue = (int)Math.round((value * k));
		}
		return iValue;
	}

	/**
	 * @param distr
	 * @return
	 */
	public static int findMean(NonParametricDistrRejectionSampler<IntRange> distr) {
		int mean = 0;
		Map<IntRange, Double> norDistr = distr.getNormDistr();
		double sum = 0;
		for (IntRange range : norDistr.keySet()) {
			sum += range.getMean() * norDistr.get(range);
		}
		mean = (int)Math.round(sum);
		return mean;
	}
	
	/**
	 * Whether the given sample distribution fits normal distr
	 * @param firstStat
	 * @param reafMean
	 * @param refStdDev
	 * @return true if fits normal
	 */
	public static Pair<Double, Double> distrFittnessNormalWithChiSquare(HistogramStat stat, double reafMean, double refStdDev, 
		double confIntervalFactor) {
		Map<Double, Double> distr = stat.getDistribution();
		double binWidth = stat.getBinWidth();
		int count = stat.getCount();
		StandardNormalDistribution stdDistr = new StandardNormalDistribution(reafMean, refStdDev);
		double sum = 0;
		for (double base : distr.keySet()) {
			double actualDistr = distr.get(base);
			double xMin = base - binWidth / 2;
			double xMax = base + binWidth / 2;
			double expectedDistr = stdDistr.getDistrBetween(xMin, xMax);
			double actualCount = actualDistr * count;
			double expectedCount = expectedDistr * count;
			double delta = actualCount - expectedCount;
			sum += delta * delta / expectedCount;
		}
		double chiSquareStat = sum;
		
		//mean and std dev calculated from the same data set
		int degOfFreedom = distr.size() - 3;
		double critValue = ChiSquareDistributionCriticalValues.getCriticalPoint(degOfFreedom, confIntervalFactor);
		//return chiSquareStat < critValue;
		return new Pair<Double, Double>(chiSquareStat, critValue);
	}
	
	/**
	 * @param refStat
	 * @param currentStat
	 * @param confIntervalFactor
	 * @return
	 */
	public static Pair<Double, Double> distrFittnessReferenceWithChiSquare(HistogramStat refStat, 
		HistogramStat currentStat, double confIntervalFactor) {
		double divergence = 0;
		Map<Double, Double> distr = refStat.getDistribution();
		int count = refStat.getCount();

		Map<Integer, Double> refDistr = roundfOffKey(refStat.getDistribution());
		Map<Integer, Double> currentDistr = roundfOffKey(currentStat.getDistribution());
		
		Set<Integer> superKeySet = new HashSet<Integer>();
		superKeySet.addAll(refDistr.keySet());
		superKeySet.addAll(currentDistr.keySet());
		
		double sum = 0;
		for (Integer key : superKeySet) {
			Double refVal = refDistr.get(key);
			Double currentVal = currentDistr.get(key); 
			double expectedCount = null != refVal? refVal * count : 0;
			double actualCount = null != currentVal ? currentVal * count : 0;
			if (0 == expectedCount) {
				++expectedCount;
				++actualCount;
			}
			
			double delta = actualCount - expectedCount;
            sum += delta * delta / expectedCount;				
		}
		double chiSquareStat = sum;
		
		//mean and std dev calculated from the same data set
		int degOfFreedom = distr.size() - 3;
		double critValue = ChiSquareDistributionCriticalValues.getCriticalPoint(degOfFreedom, confIntervalFactor);
		return new Pair<Double, Double>(chiSquareStat, critValue);
	}
	
}
