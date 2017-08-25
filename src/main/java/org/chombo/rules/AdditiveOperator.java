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

public class AdditiveOperator extends Operator {

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public AdditiveOperator(Expression root, Expression parent, String token) {
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
			if (token.equals(PLUS_OP)) {
				value = (Integer)leftVal + (Integer)rightVal;
			} else if (token.equals(MINUS_OP)) {
				value = (Integer)leftVal - (Integer)rightVal;
			}
			type = promotedType = BaseAttribute.DATA_TYPE_INT;
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_DOUBLE) && right.type.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			if (token.equals(PLUS_OP)) {
				value = (Double)leftVal + (Double)rightVal;
			} else if (token.equals(MINUS_OP)) {
				value = (Double)leftVal - (Double)rightVal;
			}
			type = promotedType = BaseAttribute.DATA_TYPE_DOUBLE;
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_INT) && right.type.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
			if (token.equals(PLUS_OP)) {
				value = (Integer)leftVal + (Double)rightVal;
			} else if (token.equals(MINUS_OP)) {
				value = (Integer)leftVal - (Double)rightVal;
			}
			type = promotedType = BaseAttribute.DATA_TYPE_DOUBLE;
		} else if (left.type.equals(BaseAttribute.DATA_TYPE_DOUBLE) && right.type.equals(BaseAttribute.DATA_TYPE_INT)) {
			if (token.equals(PLUS_OP)) {
				value = (Double)leftVal + (Integer)rightVal;
			} else if (token.equals(MINUS_OP)) {
				value = (Double)leftVal - (Integer)rightVal;
			}
			type = promotedType = BaseAttribute.DATA_TYPE_DOUBLE;
		}
		if (null == value) {
			throw new IllegalStateException("failed evaluation for additive operator");
		}
		return value;
	}

	@Override
	public int getPrecedence() {
		return ADDITIVE_PREC;
	}

}
