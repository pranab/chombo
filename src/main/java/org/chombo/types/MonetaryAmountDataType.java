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
import java.util.regex.Pattern;

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class MonetaryAmountDataType extends DataType {
	private Pattern currencyPattern;
	
	public MonetaryAmountDataType(String name,  int strength) {
		super(name, strength);
		currencyPattern = Pattern.compile(BaseAttribute.PATTERN_STR_CURRENCY);
	}
	
	@Override
	public boolean isMatched(String value) {
		boolean isMonetaryAmount = false;
		String[] items = value.trim().split("\\s+");
		if (items.length == 2) {
			Matcher matcher = currencyPattern.matcher(items[0].toUpperCase());
			isMonetaryAmount = matcher.matches() && BasicUtils.isFloat(items[1]);
		}
		
		return isMonetaryAmount;
	}
}
