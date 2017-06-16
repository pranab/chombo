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

import java.util.HashMap;
import java.util.Map;

import org.chombo.util.BasicUtils;
import org.chombo.util.DoubleRange;

/**
 * @author pranab
 *
 */
public class DoubleRejectionSampler extends NonParametricDistrRejectionSampler<DoubleRange> {
	private Map<Integer, DoubleRange> bins = new HashMap<Integer, DoubleRange>();
	private double min; 
	private double max; 
	private double binWidth;
			
	/**
	 * @param min
	 * @param max
	 * @param binCount
	 */
	public DoubleRejectionSampler(double min, double max, int binCount){
		this.min = min;
		this.max = max;
		binWidth = (max - min) / binCount;
	}
	
	/**
	 * @param base
	 * @param value
	 */
	public void add(double base, double value) {
		int index = (int)((value - min) / binWidth);
		DoubleRange range = bins.get(index);
		if (range == null) {
			double binMin = index * binWidth;
			double binMax = (index + 1) * binWidth;
			range = new DoubleRange(binMin, binMax);
			bins.put(index, range);
		}
		add(range, value);
	}
	
	/**
	 * @return
	 */
	public double sampleDouble() {
		DoubleRange range = sample();
		double sampled = BasicUtils.sampleUniform(range.getLeft(), range.getRight());
		return sampled;
	}
}
