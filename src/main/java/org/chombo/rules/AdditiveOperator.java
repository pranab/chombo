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
import org.chombo.util.BasicUtils;

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
		BasicUtils.assertCondition(children.size() >= 2, "need at least 2 operands");
		boolean first = true;
		if (anyDouble()) {
			double dValue = 0;
			for (Expression child : children) {
				double childVal = (Double)child.evaluate();
				if (token.equals(PLUS_OP)) {
					dValue += childVal;
				} else if (token.equals(MINUS_OP)) {
					if (first) {
						dValue += childVal;
						first = false;
					} else {
						dValue -= childVal;
					}
				}
			}	
			value = dValue;
		} else {
			int iValue = 0;
			for (Expression child : children) {
				int childVal = (Integer)child.evaluate();
				if (token.equals(PLUS_OP)) {
					iValue += childVal;
				} else if (token.equals(MINUS_OP)) {
					if (first) {
						iValue += childVal;
						first = false;
					} else {
						iValue -= childVal;
					}
				}
			}			
			value = iValue;
		}
		
		return value;
	}

	/**
	 * @return
	 */
	private boolean anyDouble() {
		boolean isDouble = false;
		for (Expression child : children) {
			if (child.type.equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
				isDouble = true;
				break;
			}
		}
		return isDouble;
	}
	
	@Override
	public int getPrecedence() {
		return ADDITIVE_PREC;
	}
	
	/**
	 * @return
	 */
	public boolean isMultiOperand() {
		return true;
	}

}
