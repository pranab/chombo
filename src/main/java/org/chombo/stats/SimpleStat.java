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

import org.chombo.util.BasicUtils;


/**
 * @author pranab
 *
 */
public class SimpleStat extends MeanStat {
	protected double sumSq;
	protected double  stdDev;
	
	public SimpleStat() {
	}
	
	/**
	 * @param count
	 * @param sum
	 * @param mean
	 */
	public SimpleStat(int count, double sum,  double sumSq, double mean, double stdDev) {
		super(count, sum,  mean);
		this.sumSq = sumSq;
		this.stdDev = stdDev;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.MeanStat#initialize()
	 */
	@Override
	public void initialize() {
		super.initialize();
		sumSq = 0;
		stdDev = 0;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.MeanStat#add(double)
	 */
	@Override
	public void add(double value) {
		super.add(value);
		sumSq += value * value;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.AverageValue#getMean()
	 */
	@Override
	public double getMean() {
		if (!processed) {
			super.getMean();
			stdDev = Math.sqrt(sumSq / count - mean * mean);
		}
		return mean;
	}

	/**
	 * @return
	 */
	public double getStdDev() {
		if (!processed) {
			super.getMean();
			stdDev = Math.sqrt(sumSq / count - mean * mean);
		}
		return stdDev;
	}

	/* (non-Javadoc)
	 * @see org.chombo.stats.AverageValue#setMean(double)
	 */
	@Override
	public void setMean(double mean) {
		this.mean = mean;
		processed = true;
	}
	
	/**
	 * @param count
	 * @param avgValue
	 * @param stdDev
	 */
	public void setStats(int count, double mean, double stdDev) {
		this.count = count;
		this.mean = mean;
		this.stdDev = stdDev;
		processed = true;
	}
	
	/**
	 * @param count
	 */
	public void setCount(int count) {
		this.count = count;
	}
		
	public int getCount() {
		return count;
	}

	public double getSumSq() {
		return sumSq;
	}

	public void setSumSq(double sumSq) {
		this.sumSq = sumSq;
	}

	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder stBld = new StringBuilder();
		stBld.append(count).append(delim).append(BasicUtils.formatDouble(sum)).append(delim).
		append(sumSq).append(delim).append(mean).append(delim).append(stdDev);
		return stBld.toString();
	}
	
	/**
	 * @param rec
	 */
	public void fromString(String rec) {
		String[] items = rec.split(delim, -1);
		int i = 0;
		count = Integer.parseInt(items[i++]);
		sum = Double.parseDouble(items[i++]);
		sumSq = Double.parseDouble(items[i++]);
		mean = Double.parseDouble(items[i++]);
		stdDev = Double.parseDouble(items[i]);
		processed = true;
	}
	
}
