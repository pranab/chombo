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

/**
 * @author pranab
 *
 */
public class Operator extends Expression {
	public static final String RULE_OP = "rule";
	public static final String IF_OP = "if";
	public static final String THEN_OP = "then";
	public static final String OR_OP = "or";
	public static final String AND_OP = "and";
	public static final String LESS_THAN_OP = "<";
	public static final String LESS_THAN_EQUAL_TO_OP = "<=";
	public static final String GREATER_THAN_OP = ">";
	public static final String GREATER_THAN_EQUAL_TO_OP = ">=";
	public static final String EQUAL_TO_OP = "=";
	public static final String EQUAL_TO_STRING_OP = "eq";
	public static final String PLUS_OP = "+";
	public static final String MINUS_OP = "-";
	public static final String MULTIPLY_OP = "*";
	public static final String DIVIDE_OP = "/";
	public static final String PARENTHESIS_OP = "(";
	
	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public Operator(Expression root, Expression parent, String token) {
		super(root,  parent,  token);
	}
	
	protected String operator;
}
