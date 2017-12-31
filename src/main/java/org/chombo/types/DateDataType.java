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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class DateDataType extends DataType {
	private List<SimpleDateFormat> formatList = new ArrayList<SimpleDateFormat>();
	
	/**
	 * @param name
	 * @param formatStringList
	 * @param strength
	 */
	public DateDataType(String name, List<String> formatStringList, int strength) {
		super(name, strength);
		for (String formatString : formatStringList) {
			formatList.add(new SimpleDateFormat(formatString));
		}
	}

	@Override
	public boolean isMatched(String value) {
		boolean isDate = false;
		value = value.trim();
	    for (SimpleDateFormat dateFormat : formatList) {
    		//date if at least 1 format is able to parse
    		isDate = BasicUtils.isDate(value, dateFormat);
    		if (isDate)
    			break;
    	}
		return isDate;
	}
}
