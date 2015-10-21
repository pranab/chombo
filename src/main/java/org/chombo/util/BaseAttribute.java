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
public class BaseAttribute {
	protected String name;
	protected int ordinal = -1;
	protected String dataType;
	public static final String DATA_TYPE_STRING = "string";
	public static final String DATA_TYPE_CATEGORICAL = "categorical";
	public static final String DATA_TYPE_INT = "int";
	public static final String DATA_TYPE_LONG = "long";
	public static final String DATA_TYPE_DOUBLE = "double";
	public static final String DATA_TYPE_TEXT = "text";
	public static final String DATA_TYPE_DATE = "date";
	
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
	public boolean isCategorical() {
		return dataType.equals(DATA_TYPE_CATEGORICAL);
	}

	public boolean isInteger() {
		return dataType.equals(DATA_TYPE_INT );
	}

	public boolean isLong() {
		return dataType.equals(DATA_TYPE_LONG );
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

}
