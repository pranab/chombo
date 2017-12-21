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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author pranab
 *
 */
public class DataType implements Serializable, Comparable<DataType> {
	protected String name;
	protected String patternStr;
	protected int strength;
	protected Pattern pattern;

	public DataType() {
	}
	
	public DataType(String name, int strength) {
		super();
		this.name = name;
		this.strength = strength;
	}
	
	public DataType(String name, String patternStr, int strength) {
		super();
		this.name = name;
		this.patternStr = patternStr;
		this.strength = strength;
		pattern = Pattern.compile(patternStr);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPatternStr() {
		return patternStr;
	}

	public void setPatternStr(String patternStr) {
		this.patternStr = patternStr;
		pattern = Pattern.compile(patternStr);
	}

	public int getStrength() {
		return strength;
	}

	public void setStrength(int strength) {
		this.strength = strength;
	}
	
	public boolean isMatched(String value) {
		Matcher matcher = pattern.matcher(value.trim());
		return matcher.matches();
	}
	
	@Override
	public int compareTo(DataType that) {
		return this.strength < that.strength ? -1 : (this.strength > that.strength ? 1 : 0);
	}
}
