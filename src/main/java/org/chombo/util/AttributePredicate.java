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

import java.util.Map;

/**
 * @author pranab
 *
 */
public abstract class AttributePredicate extends BasePredicate{

	public static final String GREATER_THAN = "gt";
	public static final String LESS_THAN = "lt";
	public static final String EQUAL_TO = "eq";
	public static final String IN = "in";
	public static final String NOT_IN = "ni";
	public static final String PREDICATE_SEP = "\\s+";
	public static final String DATA_TYPE_SEP = ":";
	public static final String VALUE_LIST_SEP = "\\|";
	
	/**
	 * 
	 */
	public AttributePredicate() {
	}
	
	/**
	 * @param attribute
	 * @param operator
	 */
	public AttributePredicate(int attribute, String operator) {
		super(attribute, operator);
	}

	/**
	 * @param predicateStr
	 * @return
	 */
	public static AttributePredicate create(String predicateStr) {
		return create(predicateStr, null);
	}	
	
	/**
	 * @param predicateStr
	 * @param context
	 * @return
	 */
	public static AttributePredicate create(String predicateStr, Map<String, Object> context) {
		AttributePredicate  predicate = null;
		String[] predParts = predicateStr.trim().split(AttributePredicate.PREDICATE_SEP);
		int attr = Integer.parseInt(predParts[0]);
		String[] valueParts  = predParts[2].split(AttributePredicate.DATA_TYPE_SEP);
		
		if (valueParts[0].equals(BaseAttribute.DATA_TYPE_INT)) {
			predicate = new IntAttributePredicate(attr, predParts[1], valueParts[1]);
		} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			predicate = new DoubleAttributePredicate(attr, predParts[1], valueParts[1]);
		} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_STRING)) {
			if (null != context) {
				predicate = new StringAttributePredicate();
				predicate.withContext(context);
				predicate.build(attr, predParts[1], valueParts[1]);
			} else {
				predicate = new StringAttributePredicate(attr, predParts[1], valueParts[1]);
			}
		} else {
			throw new IllegalArgumentException("invalid data type");
		}
		return predicate;
	}
	
	/**
	 * @param attribute
	 * @param operator
	 * @param value
	 */
	//public abstract void build(int attribute, String operator, String value);

	/**
	 * evaluates predicate 
	 * @param record
	 * @return
	 */
	//public abstract boolean evaluate(String[] record);

	/**
	 * evaluates predicate 
	 * @param field
	 * @return
	 */
	//public abstract boolean evaluate(String field);
	
}
