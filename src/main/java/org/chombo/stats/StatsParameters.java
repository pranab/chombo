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
public final class StatsParameters {
	private double mean;
	private double stdDev;
	private double skew;
	private double median;
	private double medAbsDivergence;
	private double quaterPercentile;
	private double threeQuaterPercentile;
	private double min;
	private double max;
	
	public double getMean() {
		return mean;
	}
	public void setMean(double mean) {
		this.mean = mean;
	}
	public double getStdDev() {
		return stdDev;
	}
	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}
	public double getSkew() {
		return skew;
	}
	public void setSkew(double skew) {
		this.skew = skew;
	}
	public double getMedian() {
		return median;
	}
	public void setMedian(double median) {
		this.median = median;
	}
	public double getMedAbsDivergence() {
		return medAbsDivergence;
	}
	public void setMedAbsDivergence(double medAbsDivergence) {
		this.medAbsDivergence = medAbsDivergence;
	}
	public double getQuaterPercentile() {
		return quaterPercentile;
	}
	public void setQuaterPercentile(double quaterPercentile) {
		this.quaterPercentile = quaterPercentile;
	}
	public double getThreeQuaterPercentile() {
		return threeQuaterPercentile;
	}
	public void setThreeQuaterPercentile(double threeQuaterPercentile) {
		this.threeQuaterPercentile = threeQuaterPercentile;
	}
	public double getMin() {
		return min;
	}
	public void setMin(double min) {
		this.min = min;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
	}
	
}
