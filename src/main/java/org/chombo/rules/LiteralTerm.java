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

import javax.naming.directory.BasicAttribute;

import org.chombo.util.BaseAttribute;

/**
 * @author pranab
 *
 */
public class LiteralTerm extends Term {

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public LiteralTerm(Expression root, Expression parent, String token) {
		super(root, parent, token);
	}
	
	@Override
	public Object evaluate() {
		try {
			value = Integer.parseInt(token);
			type = promotedType = BaseAttribute.DATA_TYPE_INT;
		} catch (Exception iEx) {
			try {
				value = Double.parseDouble(token);
				type = promotedType = BaseAttribute.DATA_TYPE_DOUBLE;
			} catch (Exception dEx) {
				value = token;
				type = promotedType = BaseAttribute.DATA_TYPE_STRING;
			}
		}
		return value;
	}
	
	@Override
	public int getPrecedence() {
		return TERM_PREC;
	}

}
