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
public class NormalDistrRejectionSampler  implements RejectionSampler<Double>{
	private double mean;
	private double stdDev;
	private double min;
	private double max;
	private double con;
	
	/**
	 * @param mean
	 * @param stdDev
	 * @param rangeStdDev
	 */
	public NormalDistrRejectionSampler(double mean, double stdDev, double rangeStdDev) {
		this.mean = mean;
		this.stdDev = stdDev;
		min = mean - rangeStdDev * stdDev;
		max = mean + rangeStdDev * stdDev;
		con = stdDev * Math.pow(2 * Math.PI, 0.5);
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.util.RejectionSampler#sample()
	 */
	public Double sample() {
		Double sampled = 0.0;
		while(true) {
			double base = BasicUtils.sampleUniform(min, max);
			double temp = (base - mean) / stdDev;
			double distr = Math.exp(-0.5 * temp * temp) / con;
			if (Math.random() < distr) {
				sampled = base;
				break;
			}
		}
		
		return sampled;
	}

}
