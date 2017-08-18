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
 * Holds string and value type
 * @author pranab
 *
 */
public class ValueType  extends Pair<String, String> {
	
	public ValueType(String value, String type) {
		super(value, type);
	}
	
	public String getType() {
		return right;
	}
	
	public String getStringValue() {
		return left;
	}
	
	public int getIntValue() {
		int value = 0;
		if (right.equals(BaseAttribute.DATA_TYPE_INT)) {
			value = Integer.parseInt(left);
		} else {
			throw new IllegalStateException("data type is not int");
		}
		return value;
	}
	
	public double getDoubleValue() {
		double value = 0;
		if (right.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			value =  Double.parseDouble(left);
		} else {
			throw new IllegalStateException("data type is not double");
		}
		return value;
	}
}
