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
 * Poison distribution
 * @author pranab
 *
 */
public class PoisonDistr {
	private double avCount;
	private int curCount;
	private double distr;
	
	/**
	 * @param avCount
	 */
	public PoisonDistr(double avCount) {
		super();
		this.avCount = avCount;
		distr = Math.exp(-avCount);
	}
	
	/**
	 * 
	 */
	public void initialize() {
		curCount = 0;
		distr = Math.exp(-avCount);
	}
	
	/**
	 * @return
	 */
	public double next() {
		double curDistr = distr;
		++curCount;
		distr *=  (avCount / curCount);
		return curDistr;
	}
	
	/**
	 * @param count
	 * @return
	 */
	public double get(int count) {
		double distr = Math.exp(-avCount);
		if (count > 0) {
			distr *= Math.pow(avCount, count) / BasicUtils.factorial(count);
		}
		return distr;
	}
}
