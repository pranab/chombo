
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.chombo.util.BaseAttribute;

/**
 * @author pranab
 *
 */
public class DataTypeHandler implements Serializable {
	private List<DataType> dataTypes = new ArrayList<DataType>();
	
	/**
	 * 
	 */
	public DataTypeHandler() {
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_CURRENCY, BaseAttribute.PATTERN_STR_CURRENCY, 90));
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_STATE, BaseAttribute.PATTERN_STR_STATE, 85));
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_PHONE_NUM, BaseAttribute.PATTERN_STR_PHONE_NUM, 80));
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_ZIP, BaseAttribute.PATTERN_STR_ZIP, 80));
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_STREET_ADDRESS, BaseAttribute.PATTERN_STR_STREET_ADDRESS, 75));
		dataTypes.add(new DataType(BaseAttribute.DATA_TYPE_CITY, BaseAttribute.PATTERN_STR_CITY, 70));
		Collections.sort(dataTypes);
	}
	
	/**
	 * @param formatStringList
	 */
	public void addDateType(List<String> formatStringList) {
		dataTypes.add(new DateDataType(BaseAttribute.DATA_TYPE_DATE, formatStringList, 84));
		Collections.sort(dataTypes);
	}
	
	/**
	 * @param name
	 * @param patternStr
	 * @param strength
	 */
	public void addCustomType(String name, String patternStr, int strength) {
		dataTypes.add(new DataType(name, patternStr, strength));
		Collections.sort(dataTypes);
	}
	
	/**
	 * @param value
	 * @return
	 */
	public String findType(String value) {
		String matchedType = null;
		for (DataType dataType : dataTypes) {
			if (dataType.isMatched(value)) {
				matchedType = dataType.name;
				break;
			}
		}
		return matchedType;
	}
}
