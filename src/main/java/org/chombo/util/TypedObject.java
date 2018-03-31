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

/**
 * Object with type indicator
 * @author pranab
 *
 */
public class TypedObject extends Pair<Object, String>  implements Serializable {
	
	/**
	 * @param value
	 * @param type
	 */
	public TypedObject(Object value, String type) {
		super(value, type);
	}
	
	/**
	 * @param value
	 */
	public TypedObject(String value) {
		super(value, BaseAttribute.DATA_TYPE_STRING);
	}

	/**
	 * @param value
	 */
	public TypedObject(Integer value) {
		super(value, BaseAttribute.DATA_TYPE_INT);
	}
	
	/**
	 * @param value
	 */
	public TypedObject(Float value) {
		super(value, BaseAttribute.DATA_TYPE_FLOAT);
	}

	/**
	 * @return
	 */
	public Object getValue() {
		return left;
	}
	/**
	 * @return
	 */
	public String getType() {
		return right;
	}

	/**
	 * @return
	 */
	public String getStringValue() {
		return (String)left;
	}
	
	/**
	 * @return
	 */
	public Integer getIntegerValue() {
		return (Integer)left;
	}
	
	/**
	 * @return
	 */
	public Float getFloatValue() {
		return (Float)left;
	}
	
	/**
	 * @return
	 */
	public boolean isString() {
		return right.equals(BaseAttribute.DATA_TYPE_STRING);
	}
	
	/**
	 * @return
	 */
	public boolean isInt() {
		return right.equals(BaseAttribute.DATA_TYPE_INT);
	}
	
	/**
	 * @return
	 */
	public boolean isFloat() {
		return right.equals(BaseAttribute.DATA_TYPE_FLOAT);
	}

}
