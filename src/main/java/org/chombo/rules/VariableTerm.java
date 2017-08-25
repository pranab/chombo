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
public class VariableTerm extends Term {

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public VariableTerm(Expression root, Expression parent, String token) {
		super(root,  parent,  token);
	}
	
	@Override
	public Object evaluate() {
		BaseAttribute attr = root.getAttribute(token);
		
		if (null == attr) {
			throw new IllegalStateException("undefined variable " + token);
		}

		//string value
		int fieldOrd = attr.getOrdinal();
		String stVal = root.input[fieldOrd];
		
		//typed value
		if (attr.isInteger()) {
			value = Integer.parseInt(stVal);
			type = promotedType = BaseAttribute.DATA_TYPE_INT;
		} else if (attr.isDouble()) {
			value = Double.parseDouble(stVal);
			type = promotedType = BaseAttribute.DATA_TYPE_DOUBLE;
		} else if (attr.isString()) {
			value = stVal;
			type = promotedType = BaseAttribute.DATA_TYPE_STRING;
		} else {
			throw new IllegalStateException("unsupported data type");
		}
		return null;
	}
	
	@Override
	public int getPrecedence() {
		return TERM_PREC;
	}

}
