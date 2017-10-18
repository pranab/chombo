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

/**
 * @author pranab
 *
 */
public class CompleteStat extends SimpleStat {
	private double min = Double.MAX_VALUE;
	private double max = Double.MIN_VALUE;
	
	public CompleteStat(){
	}
	
	/**
	 * @param count
	 * @param sum
	 * @param sumSq
	 * @param mean
	 * @param stdDev
	 */
	public CompleteStat(int count, double sum,  double sumSq, double mean, double stdDev, double min, double max) {
		super(count, sum, sumSq, mean, stdDev);
		this.min = min;
		this.max = max;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.SimpleStat#initialize()
	 */
	@Override
	public void initialize() {
		super.initialize();
		min = Double.MAX_VALUE;
		max = Double.MIN_VALUE;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.MeanStat#add(double)
	 */
	@Override
	public void add(double value) {
		super.add(value);
		if (value < min) {
			min = value;
		} 
		if (value > max) {
			max = value;
		}
	}
	
	/**
	 * @param other
	 */
	public void merge(CompleteStat other) {
		count += other.count;
		max = max > other.max ? max : other.max;
		min = min < other.min ? min : other.min;
		sum += other.sum;
		sumSq += other.sumSq;
		this.processed = false;
	}
	
	/**
	 * @return
	 */
	public double getMin() {
		return min;
	}
	
	/**
	 * @return
	 */
	public double getMax() {
		return max;
	}
}
