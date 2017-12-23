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
package org.chombo.types;

/**
 * @author pranab
 *
 */
public class IntDataType extends DataType{
	protected int min;
	protected int max;
	private boolean withLimitCheck;

	/**
	 * @param name
	 * @param strength
	 */
	public IntDataType(String name,  int strength) {
		super(name, strength);
		withLimitCheck = false;
	}

	/**
	 * @param name
	 * @param strength
	 * @param min
	 * @param max
	 */
	public IntDataType(String name, int min, int max, int strength) {
		super(name, strength);
		this.min = min;
		this.max = max;
		withLimitCheck = true;
	}
	
	@Override
	public boolean isMatched(String value) {
		boolean matched = false;
		int iVal = 0;
		try {
			iVal = Integer.parseInt(value);
			matched = withLimitCheck ? (iVal >= min && iVal <= max) : true;
		} catch (Exception ex) {
		}
		return matched;
	}

}
