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

import java.util.regex.Matcher;

/**
 * @author pranab
 *
 */
public class IdDataType extends StringDataType {
	private int length;
	
	public IdDataType(String name, String patternStr, int length, int strength) {
		super(name, patternStr, strength);
		this.length = length;
	}

	@Override
	public boolean isMatched(String value) {
		String trimmedVal = value.trim();
		Matcher matcher = pattern.matcher(trimmedVal);
		return matcher.matches() && trimmedVal.length() == length;
	}

}
