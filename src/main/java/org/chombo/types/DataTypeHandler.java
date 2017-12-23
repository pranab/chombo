
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
import java.util.Set;

import org.chombo.util.BaseAttribute;

/**
 * @author pranab
 *
 */
public class DataTypeHandler implements Serializable {
	private List<DataType> dataTypes = new ArrayList<DataType>();
	private boolean matchStrongestType;
	
	public boolean isMatchStrongestType() {
		return matchStrongestType;
	}

	public void setMatchStrongestType(boolean matchStrongestType) {
		this.matchStrongestType = matchStrongestType;
	}

	/**
	 * @param dataTypesEnabled
	 */
	public void addStringDataTypes(Set<String> dataTypesEnabled) {
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_CURRENCY))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_CURRENCY, BaseAttribute.PATTERN_STR_CURRENCY, 90));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_MONETARY_AMOUNT))
			dataTypes.add(new MonetaryAmountDataType(BaseAttribute.DATA_TYPE_MONETARY_AMOUNT,  89));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_STATE))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_STATE, BaseAttribute.PATTERN_STR_STATE, 85));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_PHONE_NUM))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_PHONE_NUM, BaseAttribute.PATTERN_STR_PHONE_NUM, 80));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_ZIP))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_ZIP, BaseAttribute.PATTERN_STR_ZIP, 80));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_STREET_ADDRESS))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_STREET_ADDRESS, BaseAttribute.PATTERN_STR_STREET_ADDRESS, 75));
		if (dataTypesEnabled.contains(BaseAttribute.DATA_TYPE_CITY))
			dataTypes.add(new StringDataType(BaseAttribute.DATA_TYPE_CITY, BaseAttribute.PATTERN_STR_CITY, 70));
	}
	
	/**
	 * @param formatStringList
	 */
	public void addDateType(List<String> formatStringList) {
		dataTypes.add(new DateDataType(BaseAttribute.DATA_TYPE_DATE, formatStringList, 84));
	}
	
	/**
	 * @param name
	 * @param patternStr
	 * @param strength
	 */
	public void addCustomStringType(String name, String patternStr, int strength) {
		//if exists already remove because patching pattern or strength is  being changed
		DataType typeToBeRemoved = null;
		for (DataType dataType : dataTypes) {
			if (dataType.name.equals(name)) {
				typeToBeRemoved = dataType;
				break;
			}
		}	
		if (null != typeToBeRemoved) {
			dataTypes.remove(typeToBeRemoved);
		}
		
		dataTypes.add(new StringDataType(name, patternStr, strength));
	}
	
	/**
	 * @param name
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addNumericTypes() {
		dataTypes.add(new IntDataType(BaseAttribute.DATA_TYPE_INT, 20));
		dataTypes.add(new IntDataType(BaseAttribute.DATA_TYPE_FLOAT, 10));
	}

	/**
	 * @param name
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addIntType(String name, int min, int max, int strength) {
		dataTypes.add(new IntDataType(name, min, max, strength));
	}
	
	/**
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addAgeType(int min, int max, int strength) {
		dataTypes.add(new IntDataType(BaseAttribute.DATA_TYPE_AGE, min, max, strength));
	}
	
	/**
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addEpochTimeType(long min, long max, int strength) {
		dataTypes.add(new LongDataType(BaseAttribute.DATA_TYPE_EPOCH_TIME, min, max, strength));
	}
	
	/**
	 * @param name
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addFloatType(String name, float min, float max, int strength) {
		dataTypes.add(new FloatDataType(name, min, max, strength));
	}
	
	/**
	 * 
	 */
	public void prepare() {
		Collections.sort(dataTypes);
	}

	/**
	 * @param value
	 * @return
	 */
	public List<String> findTypes(String value) {
		List<String> matchedTypes = new ArrayList<String>();
		for (DataType dataType : dataTypes) {
			if (dataType.isMatched(value)) {
				matchedTypes.add(dataType.name);
				if (matchStrongestType) {
					break;
				}
			}
		}
		return matchedTypes;
	}
}
