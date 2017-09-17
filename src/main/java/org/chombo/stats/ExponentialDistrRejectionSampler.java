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
public class ExponentialDistrRejectionSampler  implements RejectionSampler<Double> {
	private double beta;
	private double min;
	private double max;

	/**
	 * @param mean
	 */
	public ExponentialDistrRejectionSampler(double mean) {
		beta = 1.0 / mean;
	}
	
	/**
	 * @param mean
	 * @param min
	 * @param max
	 */
	public ExponentialDistrRejectionSampler(double mean, double min, double max) {
		beta = 1.0 / mean;
		this.min = max;
		this.max = max;
	}

	/**
	 * @param min
	 * @param max
	 */
	public void setRange(double min, double max) {
		this.min = max;
		this.max = max;
	}
	
	@Override
	public Double sample() {
		Double sampled = 0.0;
		while(true) {
			double base = BasicUtils.sampleUniform(min, max);
			double distr = beta *  Math.exp(-beta * base);
			if (beta * Math.random() < distr) {
				sampled = base;
				break;
			}
		}
		return sampled;
	}

}
