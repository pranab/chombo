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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.chombo.util.BasicUtils;

/**
 * Histogram that chnges as data gets added
 * 
 * @author pranab
 *
 */
public class HistogramStat implements Serializable {
	protected double binWidth = -1;
	protected Map<Integer, Bin> binMap = new TreeMap<Integer, Bin>();
	protected int count;
	protected double sum = 0.0;
	protected double sumSq = 0.0;
	protected int  sampleCount;
	protected boolean normalized;
	protected Map<Double, Double> histogram = new TreeMap<Double, Double>();
	protected boolean extendedOutput;
	protected int outputPrecision = 3;
	private boolean debugOn = false;
	private boolean serializeBins;
	public static String fieldDelim = ",";
	
	/**
	 * @param binWidth
	 */
	public HistogramStat() {
		super();
	}

	
	/**
	 * @param binWidth
	 */
	public HistogramStat(int binWidth) {
		super();
		this.binWidth = binWidth;
	}

	/**
	 * @param binWidth
	 */
	public HistogramStat(double binWidth) {
		super();
		this.binWidth = binWidth;
	}

	/**
	 * 
	 */
	public void initialize() {
		binMap.clear();
		histogram.clear();
		count = 0;
		sum = 0;
		sumSq = 0;
		normalized = false;
	}

	/**
	 * @param items
	 * @param offset
	 * @param normalized
	 */
	public void initialize(String[] items, int offset, boolean normalized)  {
		int histSize = Integer.parseInt(items[offset]);
		for (int i = ++offset; i < 2 * histSize; ) {
			double base = Double.parseDouble(items[i++]);
			double value = Double.parseDouble(items[i++]);
			histogram.put(base, value);
		}
		this.normalized = normalized;
	}	
	
	/**
	 * @param items
	 * @param offset
	 */
	public void initializeBins(String[] items, int offset) {
		int numBins = BasicUtils.getIntField(items, offset);
		int i = 0;
		for (i = offset + 1; i < 2 * numBins; ) {
			int binIndex = BasicUtils.getIntField(items, i++);
			int binCount = BasicUtils.getIntField(items, i++);
			Bin bin = binMap.get(binIndex);
			if (null == bin) {
				bin = new Bin(binIndex);
				binMap.put(binIndex, bin);
			}
			bin.addCount(binCount);
		}
		
		this.count = BasicUtils.getIntField(items, i++);
		this.sum = BasicUtils.getDoubleField(items, i++);
		this.sumSq = BasicUtils.getDoubleField(items, i);
	}
	
	/**
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
			String[] items = line.split(fieldDelim);
			String[] key = Arrays.copyOfRange(items, 0, keyLen);
			
			stat.initialize(items, keyLen, normalized);
			histStats.put(key, stat);
		}
		
		return histStats;
	}
	
	/**
	 * @param binWidth
	 */
	public void setBinWidth(int binWidth) {
		this.binWidth = binWidth;
	}

	/**
	 * @param binWidth
	 */
	public void setBinWidth(double binWidth) {
		this.binWidth = binWidth;
	}
	
	/**
	 * @param extendedOutput
	 * @return
	 */
	public HistogramStat withExtendedOutput(boolean extendedOutput) {
		this.extendedOutput = extendedOutput;
		return this;
	}
	
	public HistogramStat withSerializeBins(boolean serializeBins) {
		this.serializeBins = serializeBins;
		return this;
	}
	
	/**
	 * @param outputPrecision
	 * @return
	 */
	public HistogramStat withOutputPrecision(int outputPrecision) {
		this.outputPrecision = outputPrecision;
		return this;
	}
	
	/**
	 * @param outputPrecision
	 * @return
	 */
	public HistogramStat withFieldDelim(String fieldDelim) {
		HistogramStat.fieldDelim = fieldDelim;
		return this;
	}

	/**
	 * @param value
	 */
	public void add(int value) {
		add(value, 1);
	}
	

	/**
	 * @param value
	 * @param count
	 */
	public void add(int value, int count) {
		int index = (int)(value / binWidth);
		addToBin(index, value, count);
	}

	/**
	 * @param value
	 */
	public void add(long value) {
		add(value, 1);
	}
	
	/**
	 * @param value
	 * @param count
	 */
	public void add(long value, int count) {
		int index = (int)(value / binWidth);
		addToBin(index, value, count);
	}

	/**
	 * @param value
	 */
	public void add(float value) {
		add(value, 1);
	}
	
	/**
	 * @param value
	 * @param count
	 */
	public void add(float value, int count) {
		int index = (int)(value / binWidth);
		addToBin(index, value, count);
	}
	
	
	/**
	 * @param value
	 */
	public void add(double value) {
		add(value, 1);
	}
	
	/**
	 * @param value
	 * @param count
	 */
	public void add(double value, int count) {
		int index = (int)(value / binWidth);
		addToBin(index, value, count);
	}
	
	/**
	 * @param index
	 * @param value
	 */
	private void addToBin(int index, double value, int count) {
		if (debugOn) {
			System.out.println("index: " + index + " value: " + BasicUtils.formatDouble(value, outputPrecision) + 
					" count: " + count);
		}
		Bin bin = binMap.get(index);
		if (null == bin) {
			bin = new Bin(index);
			binMap.put(index, bin);
		}
		bin.addCount(count);
		this.count += count;
		sum += value * count;
		sumSq += value * value * count;
		++sampleCount;
	}
	
	/**
	 * @param index
	 * @param count
	 */
	public void addBin(int index, int count) {
		Bin bin = binMap.get(index);
		if (null == bin) {
			bin = new Bin(index);
			binMap.put(index, bin);
		}
		bin.addCount(count);

		this.count += count;
		double value = index * binWidth;
		sum += value * count;
		sumSq += value * value * count;
	}
	
	/**
	 * @return
	 */
	public int getMeanCount() {
		int sum = 0;
		for (Integer index : binMap.keySet()) {
			Bin bin = binMap.get(index);
			sum += bin.count;
		}
		return sum / binMap.size();
	}

	/**
	 * @param confidenceLimitPercent
	 * @return
	 */
	public double[] getConfidenceBounds(double confidenceLimitPercent) {
		double[] confidenceBounds = new double[2];
		
		int mean = (int)getMean();
		int meanIndex = (int)(mean / binWidth);
		int confCount = 0;
		int confidenceLimit = (int)((count * confidenceLimitPercent) / 100);
		int binCount = 0;
		Bin bin = binMap.get(meanIndex);
		if (null != bin) {
			confCount += bin.getCount();
			++binCount;
		}
		
		//starting for mean index extend to both sides to include other bins
		int offset = 1;
		for(; binCount < binMap.size() ; ++offset) {
			bin = binMap.get(meanIndex + offset);
			if (null != bin) {
				confCount += bin.getCount();
				++binCount;
			}
			bin = binMap.get(meanIndex - offset);
			if (bin != null) {
				confCount += bin.getCount();
				++binCount;
			}
			if (confCount >= confidenceLimit) {
				break;
			}
		}
		
		double avBinWidth = binWidth > 1 ? 0.5 : 0.0;
		confidenceBounds[0] = (((double)(meanIndex - offset)) + avBinWidth) * binWidth;
		confidenceBounds[1] = (((double)(meanIndex + offset)) + avBinWidth) * binWidth;
		return confidenceBounds;
	}

	/**
	 * @return
	 */
	public double getMean() {
		double mean = sum / count;
		return mean;
	}
	
	/**
	 * @return
	 */
	public double getStdDev() {
		double mean = getMean();
		double stdDev = Math.sqrt(sumSq / count - mean * mean);
		return stdDev;
	}
	
	/**
	 * @return
	 */
	public int getCount() {
		return count;
	}
	
	/**
	 * @return
	 */
	public HistogramStat.Bin[] getSortedBins() {
		Bin[] bins = new Bin[binMap.size()];
		int i = 0;
		for (Integer index : binMap.keySet()) {
			Bin bin = binMap.get(index);
			bins[i++] = bin;
		}		
		//Arrays.sort(bins);
		return bins;
	}
	
	/**
	 * @return
	 */
	public HistogramStat.Bin[] getSortedBinsByCount() {
		//sort by count
		Map<Integer, List<HistogramStat.Bin>> binSotredByCount = new TreeMap<Integer, List<HistogramStat.Bin>>();
		for (Integer index : binMap.keySet()) {
			Bin bin = binMap.get(index);
			List<HistogramStat.Bin> binList = binSotredByCount.get(bin.count);
			if (null == binList) {
				binList = new ArrayList<HistogramStat.Bin>();
				binSotredByCount.put(bin.count, binList);
			}
			binList.add(bin);
		}
		
		//into an array
		Bin[] bins = new Bin[binMap.size()];
		int i = 0;
		for (Integer count : binSotredByCount.keySet()) {
			List<HistogramStat.Bin> binList = binSotredByCount.get(count);
			for (HistogramStat.Bin bin : binList) {
				bins[i++] = bin;
			}
		}		
		
		return bins;
	}

	/**
	 * @return
	 */
	public double getMedian() {
		return getQuantile(0.5);
	}
	
	/**
	 * @param quantile
	 * @return
	 */
	public double getQuantile(double quantile) {
		double median = 0;
		int quantileCount = (int)(count * quantile);

		int curCount = 0;
		Bin bin = null;
		for (int binIndex: binMap.keySet()) {
			curCount += binMap.get(binIndex).count;
			if (curCount > quantileCount) {
				bin = binMap.get(binIndex);
				break;
			}
		}
		
		//assume uniform distribution within bin
		median = bin.index * binWidth;
		int prevCount = curCount - bin.count;
		median += (binWidth * (quantileCount - prevCount)) / bin.count;
		return median;
	}
	
	/**
	 * @return
	 */
	public double getMode() {
		double mode = 0;
		int maxCount = 0;
		Bin maxBin = null;
		for (int binIndex: binMap.keySet()) {
			int thisCount = binMap.get(binIndex).count;
			if (thisCount > maxCount) {
				maxCount = thisCount;
				maxBin = binMap.get(binIndex);
			}
		}		
		
		//average within bin
		mode = maxBin.index * binWidth + binWidth / 2;
		return mode;
	}

	/**
	 * @return
	 */
	public Map<Double, Double> getDistribution() {
		if (histogram.isEmpty()) {
			for (Integer index : binMap.keySet()) {
				double val = index * binWidth + binWidth / 2;
				histogram.put(val,  ((double)binMap.get(index).count) / count);
			}
			normalized = true;
		}
		return histogram;
	}
	
	/**
	 * @param base
	 * @return
	 */
	public double findDistr(double base) {
		double minDiff = Double.MAX_VALUE;
		double distr = 0;
		for (double thisBase : histogram.keySet()) {
			double diff = Math.abs(thisBase - base);
			if (diff < minDiff) {
				minDiff = diff;
				distr = histogram.get(thisBase);
			}
		}
		return distr;
	}
	
	/**
	 * 
	 */
	public void normalizeCount() {
		for (Integer index : binMap.keySet()) {
			Bin bin = binMap.get(index);
			bin.normalize(count);
		}		
	}
	
	/**
	 * @return
	 */
	public double getEntropy() {
		double entropy = 0;
		getDistribution();
		for (double val : histogram.keySet()) {
			double distrVal = histogram.get(val);
			entropy -= distrVal * Math.log(distrVal);
		}
		return entropy;
	}
	
	public double getSum() {
		return sum;
	}


	/**
	 * @param histStat
	 * @return
	 */
	public HistogramStat merge(HistogramStat histStat) {
		HistogramStat mergedHistStat = new HistogramStat();
		mergedHistStat.binWidth = binWidth;
		mergedHistStat.extendedOutput = extendedOutput;
		mergedHistStat.outputPrecision = outputPrecision;
		
		//bins
		for (Integer index : binMap.keySet()) {
			Bin bin = binMap.get(index);
			mergedHistStat.addBin(index, bin.count);
		}
		for (Integer index : histStat.binMap.keySet()) {
			Bin bin = histStat.binMap.get(index);
			mergedHistStat.addBin(index, bin.count);
		}
		
		if (debugOn) {
			System.out.println("merging histogram " + binsToString());
			System.out.println("merging histogram " + histStat.binsToString());
			System.out.println("merged histogram " +  mergedHistStat.binsToString());
		}
		
		//other stats
		mergedHistStat.count = count + histStat.count;
		mergedHistStat.sum = sum + histStat.sum;
		mergedHistStat.sumSq = sumSq + histStat.sumSq;
		mergedHistStat.sampleCount = sampleCount + histStat.sampleCount;
		
		return mergedHistStat;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return serializeBins ? toStringBins() : toStringDistr();
	}
	
	/**
	 * @return
	 */
	public String toStringBins() {
		StringBuilder stBld = new StringBuilder(binsToString());
		stBld.append(fieldDelim).append(count).append(fieldDelim).
			append(BasicUtils.formatDouble(sum, outputPrecision)).append(BasicUtils.formatDouble(sumSq, outputPrecision));
		return stBld.toString();
	}
	
	/**
	 * @return
	 */
	public String toStringDistr() {
		StringBuilder stBld = new StringBuilder(normalizedBinsToString());
		//formatting
    	String formatter = "%." + outputPrecision + "f";
		
		//other stats
		if (extendedOutput) {
			String formMean = BasicUtils.formatDouble(getMean(), formatter);
			String formMedian = BasicUtils.formatDouble(getMedian(), formatter);
			String formStdDev = BasicUtils.formatDouble(getStdDev(), formatter);
			String formMode = BasicUtils.formatDouble(getMode(), formatter);
			String formQuartPer = BasicUtils.formatDouble(getQuantile(0.25), formatter);
			String formHalfPer = BasicUtils.formatDouble(getQuantile(0.50), formatter);
			String formThreeQuartPer = BasicUtils.formatDouble(getQuantile(0.75), formatter);
			
			stBld.append(formMean).append(fieldDelim).append(formMedian).append(fieldDelim).
				append(formStdDev).append(fieldDelim).append(formMode).append(fieldDelim).
				append(formQuartPer).append(fieldDelim).append(formHalfPer).append(fieldDelim).
				append(formThreeQuartPer).append(fieldDelim);
		}
		return stBld.substring(0, stBld.length() - 1);
	}
	
	/**
	 * @return
	 */
	public String normalizedBinsToString() {
		StringBuilder stBld = new StringBuilder();
		if (!normalized) {
			getDistribution();
		}
		
		//formatting
    	String formatter = "%." + outputPrecision + "f";

		//distribution
		stBld.append(histogram.size()).append(fieldDelim);
		for(double x : histogram.keySet()) {
			double y = histogram.get(x);
			stBld.append(BasicUtils.formatDouble(x, formatter)).append(fieldDelim).
				append(BasicUtils.formatDouble(y, formatter)).append(fieldDelim);
		}
		return stBld.substring(0, stBld.length() - 1);
	}
	
	/**
	 * @return
	 */
	public String binsToString() {
		StringBuilder stBld = new StringBuilder();

		//distribution
		stBld.append(binMap.size()).append(fieldDelim);
		for(int x : binMap.keySet()) {
			Bin y = binMap.get(x);
			stBld.append(x).append(fieldDelim).append(y.count).append(fieldDelim);
		}
		return stBld.substring(0, stBld.length() - 1);
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class Bin implements  Comparable<Bin>,  Serializable {
		private int index;
		private int count;
		private double normCount;

		public Bin(int index) {
			super();
			this.index = index;
		}

		public Bin(int index, int count) {
			super();
			this.index = index;
			this.count = count;
		}
		
		public void addCount(int count) {
			this.count += count;
		}

		@Override
		public int compareTo(Bin that) {
			return this.index < that.index ?  -1 : (this.index > that.index ? 1 : 0);
		}

		public int getIndex() {
			return index;
		}

		public int getCount() {
			return count;
		}
		
		
		public double getNormCount() {
			return normCount;
		}

		public void normalize(int maxCount) {
			normCount = ((double)count) / maxCount;
		}
	}

}
