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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * @author pranab
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseAttribute  implements Serializable {
	private static final long serialVersionUID = 7639336747752713872L;
	protected String name;
	protected int ordinal = -1;
	protected String dataType;
	protected String datePattern;
	
	public static final String DATA_TYPE_STRING = "string";
	public static final String DATA_TYPE_CATEGORICAL = "categorical";
	public static final String DATA_TYPE_INT = "int";
	public static final String DATA_TYPE_LONG = "long";
	public static final String DATA_TYPE_FLOAT = "float";
	public static final String DATA_TYPE_DOUBLE = "double";
	public static final String DATA_TYPE_BOOLEAN = "boolean";
	public static final String DATA_TYPE_TEXT = "text";
	public static final String DATA_TYPE_DATE = "date";
	public static final String DATA_TYPE_EPOCH_TIME = "epochTime";
	public static final String DATA_TYPE_SSN = "SSN";
	public static final String DATA_TYPE_PHONE_NUM = "phoneNumber";
	public static final String DATA_TYPE_AGE = "age";
	public static final String DATA_TYPE_STREET_ADDRESS = "streetAddress";
	public static final String DATA_TYPE_CITY = "city";
	public static final String DATA_TYPE_ZIP = "zip";
	public static final String DATA_TYPE_STATE = "state";
	public static final String DATA_TYPE_CURRENCY = "currency";
	public static final String DATA_TYPE_MONETARY_AMOUNT = "monetaryAmount";
	public static final String DATA_TYPE_ID = "id";
	public static final String DATA_TYPE_ID_SHORT = "idShort";
	public static final String DATA_TYPE_ID_MEDIUM = "idMedium";
	public static final String DATA_TYPE_ID_LONG = "idLong";
	public static final String DATA_TYPE_STRING_COMPOSITE = "stringComp";
	public static final String DATA_TYPE_ANY = "any";
	public static final String DATA_TYPE_GEO_LOCATION = "geoLocation";
	
	public static final String PATTERN_STR_SSN = "^(\\d{3}( |-)?\\d{2}( |-)?\\d{4}|\\w{3}( |-)?\\w{2}( |-)?(\\w){4})$";
	public static final String PATTERN_STR_PHONE_NUM = "^(\\(?\\d{3}\\)?[ \\.-]?\\d{3}[ \\.-]?\\d{4})$";
	public static final String PATTERN_STR_STREET_ADDRESS = "^(\\d{1,8}(\\s{1,4}\\S{1,20})+)$";
	public static final String PATTERN_STR_CITY = "^([A-Za-z]{2,20}(\\s{1,4}[A-Za-z]{2,20})*)$";
	public static final String PATTERN_STR_ZIP = "^(\\d{5}([-\\s]\\d{4})?)$";
	public static final String PATTERN_STR_STATE ="^(AK|AL|AR|AZ|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)$";	
 	public static final String PATTERN_STR_CURRENCY = "^(AUD|BRL|CAD|DKKEUR|HKD|INR|NOK|FKP|RUB|SGD|CHF|JPY|USD)$";
 	public static final String PATTERN_STR_BOOLEAN = "^(TRUE|FALSE|T|F|YES|NO|Y|N|0|1)$";
 	public static final String PATTERN_STR_ID = "^([a-zA-Z0-9-]{1,40})$";
 	
 	public static Map<String, String> patternStrings = new HashMap<String, String>();
 	static {
 		patternStrings.put(DATA_TYPE_SSN, PATTERN_STR_SSN);
 		patternStrings.put(DATA_TYPE_PHONE_NUM, PATTERN_STR_PHONE_NUM);
 		patternStrings.put(DATA_TYPE_STREET_ADDRESS, PATTERN_STR_STREET_ADDRESS);
 		patternStrings.put(DATA_TYPE_CITY, PATTERN_STR_CITY);
 		patternStrings.put(DATA_TYPE_STATE, PATTERN_STR_STATE);
 		patternStrings.put(DATA_TYPE_CURRENCY, PATTERN_STR_CURRENCY);
 		patternStrings.put(DATA_TYPE_BOOLEAN, PATTERN_STR_BOOLEAN);
 		patternStrings.put(DATA_TYPE_ID, PATTERN_STR_ID);
 	}
 	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinal() {
		return ordinal;
	}
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getDatePattern() {
		return datePattern;
	}
	public void setDatePattern(String datePattern) {
		this.datePattern = datePattern;
	}
	public boolean isCategorical() {
		return dataType.equals(DATA_TYPE_CATEGORICAL);
	}

	public boolean isInteger() {
		return dataType.equals(DATA_TYPE_INT );
	}

	public boolean isLong() {
		return dataType.equals(DATA_TYPE_LONG );
	}

	public boolean isFloat() {
		return dataType.equals(DATA_TYPE_FLOAT);
	}

	public boolean isDouble() {
		return dataType.equals(DATA_TYPE_DOUBLE);
	}

	public boolean isNumerical() {
		return dataType.equals(DATA_TYPE_INT ) || dataType.equals(DATA_TYPE_LONG ) || dataType.equals(DATA_TYPE_DOUBLE);
	}
	
	public boolean isString() {
		return dataType.equals(DATA_TYPE_STRING );
	}
	
	public boolean isText() {
		return dataType.equals(DATA_TYPE_TEXT );
	}

	public boolean isDate() {
		return dataType.equals(DATA_TYPE_DATE);
	}

	public boolean isGeoLocation() {
		return dataType.equals(DATA_TYPE_GEO_LOCATION );
	}
	
	public static String getPatternString(String patternName) {
		return patternStrings.get(patternName);
	}
}
