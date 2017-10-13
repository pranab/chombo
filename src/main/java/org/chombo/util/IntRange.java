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
 * A range that can be expanded
 * @author pranab
 *
 */
public class IntRange  extends Pair<Integer, Integer> implements Domain {
	private boolean isPoint; 
	private String delim;
	
	public IntRange(int min, int max) {
		super(min, max);
	}
	
	/**
	 * @param stVal
	 * @param delim
	 */
	public IntRange(String stVal, String delim) {
		String[] items = stVal.split(delim);
		if (items.length == 1) {
			int val = Integer.parseInt(stVal);
			initialize(val, val);
			isPoint = true;
		} else {
			left = Integer.parseInt(items[0]);
			right = Integer.parseInt(items[1]);
		}
		this.delim = delim;
	}
	
	/**
	 * @param value
	 */
	public void add(int value) {
		if (modify(value) && isPoint) {
			isPoint = false;
		}
	}
	
	/**
	 * @param value
	 */
	public void add(String value) {
		int iValue = Integer.parseInt(value);
		add(iValue);
	}
	
	/**
	 * @param value
	 * @return
	 */
	private boolean modify(int value) {
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
		int iValue = Integer.parseInt(value);
		return iValue >= left && iValue <= right;
	}
	
	/**
	 * @return
	 */
	public int getMean() {
		return (left + right) / 2;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "" + left + delim + right;
	}
}
