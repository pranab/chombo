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

/**
 * @author pranab
 *
 */
public class OrOperator extends Operator{

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public OrOperator(Expression root, Expression parent, String token) {
		super(root,  parent,  token);
	}
	
	@Override
	public Object evaluate() {
		BasicUtils.assertCondition(children.size() >= 2, "need at least 2 operands");
		boolean bValue = false;
		for (Expression child : children) {
			if (child.type.equals(BaseAttribute.DATA_TYPE_BOOLEAN)) {
				boolean  cValue = (Boolean)child.evaluate();
				bValue =  bValue || cValue;
			} else {
				BasicUtils.assertFail("incorrect operand type");
			}
		}
		value = bValue;
		return value;
	}

	@Override
	public int getPrecedence() {
		return OR_PREC;
	}
	
	/**
	 * @return
	 */
	public boolean isMultiOperand() {
		return true;
	}
	
}
