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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author pranab
 *
 */
public class StringAttributePredicate extends AttributePredicate {
	private String value;
	private Set<String> valueSet;

	/**
	 * 
	 */
	public StringAttributePredicate() {
	}
	
	/**
	 * @param attribute
	 * @param operator
	 * @param value
	 */
	public StringAttributePredicate(int attribute, String operator, String value) {
		super(attribute, operator);
		build(attribute, operator, value);
	}

	/**
	 * @param attribute
	 * @param operator
	 * @param value
	 */
	public void build(int attribute, String operator, String value) {
		this.attribute = attribute;
		this.operator = operator;
		this.value = value;
		
		if (null != context) {
			//large value set from external source
			if (null != context.get(value)) {
				//value is the name of the set
				valueSet = (Set<String>)context.get(value);
			}
		} 
		
		if (null == valueSet) {
			//set defined directly
			String[] valueItems = value.split(VALUE_LIST_SEP);
			if (valueItems.length > 1) {
				//create value set
				valueSet = new HashSet<String>();
				for (String val : valueItems) {
					valueSet.add(val);
				}
			}
		}
	}
	
	
	@Override
	public boolean evaluate(String[] record) {
		String operand = record[attribute];
		return evaluateHelper(operand);
	}

	@Override
	public boolean evaluate(String field) {
		return evaluateHelper(field);
	}

	/**
	 * @param operand
	 * @return
	 */
	private boolean evaluateHelper(String operand) {
		boolean status = false;
		if (operator.equals(GREATER_THAN)) {
			status = operand.compareTo(value) > 0;
		} else if (operator.equals(LESS_THAN)) {
			status = operand.compareTo(value) < 0;
		} else if (operator.equals(EQUAL_TO)) {
			status = operand.equals(value);
		} else if (operator.equals(IN)) {
			status = valueSet.contains(operand);
		} else if (operator.equals(NOT_IN)) {
			status = !valueSet.contains(operand);
		} else {
			throw new IllegalArgumentException("invalid operator");
		}
		return status;
	}

	@Override
	public void build(String predicate) {
		// TODO Auto-generated method stub
		
	}
}
