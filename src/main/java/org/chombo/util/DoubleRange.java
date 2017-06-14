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


package org.chombo.util;

/**
 * @author pranab
 *
 */
public class DoubleRange extends Pair<Double, Double> implements Domain {
	private boolean isPoint; 
	private String delim;
	private int outputPrecision;
	
	/**
	 * @param min
	 * @param max
	 */
	public DoubleRange(double min, double max) {
		left = min;
		right = max;
	}
	
	/**
	 * @param stVal
	 * @param delim
	 */
	public DoubleRange(String stVal, String delim, int outputPrecision) {
		String[] items = stVal.split(delim);
		if (items.length == 1) {
			double val = Double.parseDouble(stVal);
			initialize(val, val);
			isPoint = true;
		} else {
			left = Double.parseDouble(items[0]);
			right = Double.parseDouble(items[1]);
		}
		this.delim = delim;
		this.outputPrecision = outputPrecision;
	}
	
	/**
	 * @param value
	 */
	public void add(double value) {
		if (modify(value) && isPoint) {
			isPoint = false;
		}
	}
	
	/**
	 * @param value
	 */
	public void add(String value) {
		double dValue = Double.parseDouble(value);
		add(dValue);
	}

	/**
	 * @param value
	 * @return
	 */
	private boolean modify(double value) {
		boolean modified = true;
		if (value < left) {
			left = value;
		} else if (value > right) {
			right = value;
		} else {
			modified = false;
		}
		return modified;
	}

	@Override
	public boolean isContained(String value) {
		double dValue = Double.parseDouble(value);
		return dValue >= left && dValue <= right;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return BasicUtils.formatDouble(left, outputPrecision) + delim + 
				BasicUtils.formatDouble(right, outputPrecision);
	}
}
