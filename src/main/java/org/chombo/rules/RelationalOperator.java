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

package org.chombo.rules;

import org.chombo.util.BaseAttribute;

/**
 * @author pranab
 *
 */
public class RelationalOperator extends Operator {

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public RelationalOperator(Expression root, Expression parent, String token) {
		super(root,  parent,  token);
	}
	
	@Override
	public Object evaluate() {
		if (children.size() != 2) {
			throw new IllegalStateException("binary operator has invalid number of operands " + children.size());
		}
		
		Expression left = children.get(0);
		Expression right = children.get(1);
		Object leftVal = left.evaluate();
		Object rightVal = right.evaluate();
		
		value = null;
		if (left.type.equals(BaseAttribute.DATA_TYPE_INT) && right.type.equals(BaseAttribute.DATA_TYPE_INT)) {
			value = relOperator((Integer)leftVal, (Integer)rightVal, token);
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_DOUBLE) && right.type.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			value = relOperator((Double)leftVal, (Double)rightVal, token);
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_INT) && right.type.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			value = relOperator((Integer)leftVal, (Double)rightVal, token);
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_DOUBLE) && right.type.equals(BaseAttribute.DATA_TYPE_INT)) {
			value = relOperator((Double)leftVal, (Integer)rightVal, token);
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_STRING) && right.type.equals(BaseAttribute.DATA_TYPE_STRING)) {
			if (token.equals(EQUAL_TO_OP)) {
				value = ((String)leftVal).equals((String)rightVal);
			}
		}
		if (null == value) {
			throw new IllegalStateException("failed evaluation for relational operator " + left.type + "  " + right.type + "  " + token);
		}
		type = promotedType = BaseAttribute.DATA_TYPE_BOOLEAN;
		return value;
	}
	
	/**
	 * @param left
	 * @param right
	 * @param operator
	 * @return
	 */
	private boolean relOperator(int left, int right, String operator) {
		boolean result = false;
		if (operator.equals(LESS_THAN_OP)) {
			result = left < right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(GREATER_THAN_OP)) {
			result = left > right;
		} else if (operator.equals(GREATER_THAN_EQUAL_TO_OP)) {
			result = left >= right;
		} else if (operator.equals(EQUAL_TO_OP)) {
			result = left == right;
		}
		return result;
	}

	/**
	 * @param left
	 * @param right
	 * @param operator
	 * @return
	 */
	private boolean relOperator(int left, double right, String operator) {
		boolean result = false;
		if (operator.equals(LESS_THAN_OP)) {
			result = left < right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(GREATER_THAN_OP)) {
			result = left > right;
		} else if (operator.equals(GREATER_THAN_EQUAL_TO_OP)) {
			result = left >= right;
		} else if (operator.equals(EQUAL_TO_OP)) {
			result = left == right;
		}
		return result;
	}
	
	/**
	 * @param left
	 * @param right
	 * @param operator
	 * @return
	 */
	private boolean relOperator(double left, int right, String operator) {
		boolean result = false;
		if (operator.equals(LESS_THAN_OP)) {
			result = left < right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(GREATER_THAN_OP)) {
			result = left > right;
		} else if (operator.equals(GREATER_THAN_EQUAL_TO_OP)) {
			result = left >= right;
		} else if (operator.equals(EQUAL_TO_OP)) {
			result = left == right;
		}
		return result;
	}
	
	/**
	 * @param left
	 * @param right
	 * @param operator
	 * @return
	 */
	private boolean relOperator(double left, double right, String operator) {
		boolean result = false;
		if (operator.equals(LESS_THAN_OP)) {
			result = left < right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(LESS_THAN_EQUAL_TO_OP)) {
			result = left <= right;
		} else if (operator.equals(GREATER_THAN_OP)) {
			result = left > right;
		} else if (operator.equals(GREATER_THAN_EQUAL_TO_OP)) {
			result = left >= right;
		} else if (operator.equals(EQUAL_TO_OP)) {
			result = left == right;
		}
		return result;
	}
	
	@Override
	public int getPrecedence() {
		return RELATIONAL_PREC;
	}
	
}
