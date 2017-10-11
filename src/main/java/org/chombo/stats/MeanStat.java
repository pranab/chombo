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

import java.io.Serializable;

import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class MeanStat implements AverageValue, Serializable {
	protected double sum;
	protected int count;
	protected double mean = -1;
	protected boolean processed;
	protected String delim;
	
	public MeanStat(){
	}

	/**
	 * @param count
	 * @param sum
	 * @param mean
	 */
	public MeanStat(int count, double sum,  double mean) {
		super();
		this.sum = sum;
		this.count = count;
		this.mean = mean;
		processed = true;
	}
	
	/**
	 * 
	 */
	public void initialize() {
		sum = 0;
		count = 0;
		mean = 0;
		processed = false;
	}

	/**
	 * @param delim
	 * @return
	 */
	public MeanStat withDelim(String delim) {
		this.delim = delim;
		return this;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.stats.AverageValue#add(double)
	 */
	@Override
	public void add(double value) {
		sum +=  value;
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
			processed = true;
		}
		return mean;
	}
	
	/**
	 * @return
	 */
	public double getSum() {
		return sum;
	}

	@Override
	public void setMean(double mean) {
		// TODO Auto-generated method stub
		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder stBld = new StringBuilder();
		stBld.append(count).append(delim).append(BasicUtils.formatDouble(sum)).append(delim).append(mean);
		return stBld.toString();
	}

}
