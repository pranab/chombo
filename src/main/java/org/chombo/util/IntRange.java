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
public class IntRange extends Pair<Integer, Integer>{
	private boolean isPoint;
	
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
	 * @return
	 */
	private boolean modify(int value) {
		boolean modified = true;
		if (value < left) {
			left = value;
		} else if (value > left) {
			right = value;
		} else {
			modified = false;
		}
		return modified;
	}
	
}
