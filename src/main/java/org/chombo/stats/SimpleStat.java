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
public class SimpleStat implements AverageValue {
	private double sum;
	private double sumSq;
	private int count;
	private double mean = -1;
	private double  stdDev;
	private boolean processed;
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.AverageValue#add(double)
	 */
	public void add(double value) {
		sum +=  value;
		sumSq += value * value;
		++count;
		processed = false;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.AverageValue#getMean()
	 */
	@Override
	public double getMean() {
		if (!processed) {
			mean =  sum / count;
			stdDev = Math.sqrt(sumSq / count - mean * mean);
			processed = true;
		}
		return mean;
	}

	/**
	 * @return
	 */
	public double getStdDev() {
		if (!processed) {
			mean =  sum / count;
			stdDev = Math.sqrt(sumSq / count - mean * mean);
			processed = true;
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
	
}
