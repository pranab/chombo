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
public class StringAttributePredicate extends AttributePredicate {
	private String value;

	/**
	 * @param attribute
	 * @param operator
	 * @param value
	 */
	public StringAttributePredicate(int attribute, String operator, String value) {
		super(attribute, operator);
		this.value = value;
	}

	@Override
	public boolean evaluate(String[] record) {
		boolean status = false;
		String operand = record[attribute];
		if (operator.equals(GREATER_THAN)) {
			status = operand.compareTo(value) > 0;
		} else if (operator.equals(LESS_THAN)) {
			status = operand.compareTo(value) < 0;
		} else if (operator.equals(EQUAL_TO)) {
			status = operand.equals(value);
		}  else {
			throw new IllegalArgumentException("invalid operator");
		}
		return status;
	}

}
