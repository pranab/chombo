
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class DataTypeHandler implements Serializable {
	private List<DataType> dataTypes = new ArrayList<DataType>();
	private boolean matchStrongestType;
	private String[] allDataTypes = {
			BaseAttribute.DATA_TYPE_AGE, BaseAttribute.DATA_TYPE_EPOCH_TIME, BaseAttribute.DATA_TYPE_INT,
		    BaseAttribute.DATA_TYPE_FLOAT, BaseAttribute.DATA_TYPE_CURRENCY , BaseAttribute.DATA_TYPE_MONETARY_AMOUNT,
		    BaseAttribute.DATA_TYPE_DATE, BaseAttribute.DATA_TYPE_SSN, BaseAttribute.DATA_TYPE_PHONE_NUM, 
		    BaseAttribute.DATA_TYPE_ZIP,BaseAttribute.DATA_TYPE_STREET_ADDRESS,BaseAttribute.DATA_TYPE_CITY, 
		    BaseAttribute.DATA_TYPE_ID_SHORT,BaseAttribute.DATA_TYPE_ID_MEDIUM,BaseAttribute.DATA_TYPE_ID_LONG,
		    BaseAttribute.DATA_TYPE_STRING, BaseAttribute.DATA_TYPE_ANY};
	private String[] allNumericDataTypes = {BaseAttribute.DATA_TYPE_EPOCH_TIME, BaseAttribute.DATA_TYPE_AGE};
	private String[] allStringDataTypes = {
			BaseAttribute.DATA_TYPE_CURRENCY, BaseAttribute.DATA_TYPE_MONETARY_AMOUNT,
		    BaseAttribute.DATA_TYPE_DATE, BaseAttribute.DATA_TYPE_SSN, 
		    BaseAttribute.DATA_TYPE_PHONE_NUM, BaseAttribute.DATA_TYPE_ZIP, 
		    BaseAttribute.DATA_TYPE_STREET_ADDRESS,BaseAttribute.DATA_TYPE_CITY,
		    BaseAttribute.DATA_TYPE_ID_SHORT,BaseAttribute.DATA_TYPE_ID_MEDIUM, 
		    BaseAttribute.DATA_TYPE_ID_LONG};
	private String[] allIdTypes = {
			BaseAttribute.DATA_TYPE_ID_SHORT,BaseAttribute.DATA_TYPE_ID_MEDIUM,BaseAttribute.DATA_TYPE_ID_LONG};
	private List<String> customStringTypes = new ArrayList<String>();
	private List<String> customIntTypes = new ArrayList<String>();
	
	private Set<String> needsUpperCasing = new HashSet<String>();
	
	/**
	 * 
	 */
	public DataTypeHandler() {
		needsUpperCasing.add(BaseAttribute.DATA_TYPE_BOOLEAN);
		needsUpperCasing.add(BaseAttribute.DATA_TYPE_STATE);
		needsUpperCasing.add(BaseAttribute.DATA_TYPE_CURRENCY);
	}
	
	/**
	 * @return
	 */
	public boolean isMatchStrongestType() {
		return matchStrongestType;
	}

	/**
	 * @param matchStrongestType
	 */
	public void setMatchStrongestType(boolean matchStrongestType) {
		this.matchStrongestType = matchStrongestType;
	}

	/**
	 * @return
	 */
	public String[] getAllDataTypes() {
		String[] dataTypes = null;
		int offset = 0;
		if (!customStringTypes.isEmpty()) {
			dataTypes = new String[allDataTypes.length + customStringTypes.size() + customIntTypes.size()];
			BasicUtils.arrayCopy(allDataTypes, dataTypes);
			offset =  allDataTypes.length;
			BasicUtils.listToArrayCopy(customStringTypes, dataTypes, offset);
		}
		if (!customIntTypes.isEmpty()) {
			if (null == dataTypes) {
				dataTypes = new String[allDataTypes.length + customStringTypes.size() + customIntTypes.size()];
				BasicUtils.arrayCopy(allDataTypes, dataTypes);
				offset =  allDataTypes.length;
			}
			offset += customStringTypes.size();
			BasicUtils.listToArrayCopy(customIntTypes, dataTypes, offset);
		}	
		if (null == dataTypes) {
			dataTypes = allDataTypes;
		}
		return dataTypes;
	}

	/**
	 * @return
	 */
	public String[] getAllNumericDataTypes() {
		String[] intDataTypes = null;
		if (!customIntTypes.isEmpty()) {
			intDataTypes = new String[allNumericDataTypes.length + customIntTypes.size()];
			BasicUtils.arrayCopy(allNumericDataTypes, intDataTypes);
			BasicUtils.listToArrayCopy(customIntTypes, intDataTypes, allNumericDataTypes.length);
		} else {
			intDataTypes = allNumericDataTypes;
		}
		return intDataTypes;
	}

	/**
	 * @return
	 */
	public String[] getAllStringDataTypes() {
		String[] strDataTypes = null;
		if (!customStringTypes.isEmpty()) {
			strDataTypes = new String[allStringDataTypes.length + customStringTypes.size()];
			BasicUtils.arrayCopy(allStringDataTypes, strDataTypes);
			BasicUtils.listToArrayCopy(customStringTypes, strDataTypes, allStringDataTypes.length);
		} else {
			strDataTypes = allStringDataTypes;
		}
		return strDataTypes;
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
	 * @param iDLengths
	 */
	public void addIdType(List<Integer> idLengths) {
		if (idLengths.size() > allIdTypes.length) {
			throw new IllegalStateException("number of ID lengs provided is more than ID types supported");
		}
		
		for (int i = 0; i < idLengths.size(); ++i) {
			dataTypes.add(new IdDataType(allIdTypes[i], BaseAttribute.PATTERN_STR_ID, idLengths.get(i), 78));
		}
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
		
		//new type
		if (null == typeToBeRemoved) {
			customStringTypes.add(name);
		}
	}
	
	/**
	 * @param name
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addNumericTypes() {
		dataTypes.add(new IntDataType(BaseAttribute.DATA_TYPE_INT, 20));
		dataTypes.add(new FloatDataType(BaseAttribute.DATA_TYPE_FLOAT, 10));
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
	 * @param name
	 * @param min
	 * @param max
	 * @param strength
	 */
	public void addCustomIntType(String name, int min, int max, int strength) {
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
		
		dataTypes.add(new IntDataType(name, min, max, strength));
		
		//new type
		if (null == typeToBeRemoved) {
			customIntTypes.add(name);
		}
	}
	
	/**
	 * @param that
	 */
	public void mergeTypeLists(DataTypeHandler that) {
		customStringTypes.addAll(that.customStringTypes);
		customIntTypes.addAll(that.customIntTypes);
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
			String transVal = needsUpperCasing.contains(dataType.name) ? value.toUpperCase() : value;
			if (dataType.isMatched(transVal)) {
				matchedTypes.add(dataType.name);
				if (matchStrongestType) {
					break;
				}
			}
		}
		return matchedTypes;
	}
}
