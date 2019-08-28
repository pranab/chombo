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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public  class Expression implements Serializable {
	protected List<Expression>  children = new ArrayList<Expression>();
	protected Expression root;
	protected Expression parent;
	protected String token;
	protected List<? extends BaseAttribute> attributes;
	protected String[] input;
	protected String inputKey;
	protected Map<String, Object> context;
	protected Map<String, Double> keyedMeanValues;
	protected Map<String, Double> keyedStdDevValues;
	protected Object value;
	protected String type;
	protected String promotedType;
	
	public final int ROOT_PREC = 0;
	public final int RULE_PREC = 1;
	public final int IF_THEN_PREC = 2;
	public final int OR_PREC = 3;
	public final int AND_PREC = 4;
	public final int RELATIONAL_PREC = 5;
	public final int ADDITIVE_PREC = 6;
	public final int MULTIPLICATIVE_PREC = 7;
	public final int PARENTHESIS_FUNCTION_PREC = 8;
	public final int TERM_PREC = 9;
	
	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public Expression(Expression root, Expression parent, String token) {
		super();
		this.root = root;
		this.parent = parent;
		this.token = token;
	}

	public List<Expression> getChildren() {
		return children;
	}

	public void setChildren(List<Expression> children) {
		this.children = children;
	}

	public Expression getRoot() {
		return root;
	}

	public void setRoot(Expression root) {
		this.root = root;
	}

	public Expression getParent() {
		return parent;
	}

	public void setParent(Expression parent) {
		this.parent = parent;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public List<? extends BaseAttribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<? extends BaseAttribute> attributes) {
		this.attributes = attributes;
	}

	public String[] getInput() {
		return input;
	}

	public void setInput(String[] input) {
		this.input = input;
	}

	/**
	 * @param attributes
	 * @return
	 */
	public Expression withAttributes(List<? extends BaseAttribute> attributes){
		this.attributes = attributes;
		return this;
	}

	/**
	 * @param input
	 * @return
	 */
	public Expression withInput(String[] input) {
		this.input = input;
		return this;
	}
	
	/**
	 * @param input
	 * @return
	 */
	public Expression withInputKey(String inputKey) {
		this.inputKey = inputKey;
		return this;
	}

	/**
	 * @param conext
	 * @return
	 */
	public Expression withContext(Map<String, Object> context) {
		this.context = context;
		return this;
	}

	/**
	 * @param keyedMeanValues
	 * @return
	 */
	public Expression withKeyedMeanValues(Map<String, Double> keyedMeanValues) {
		this.keyedMeanValues = keyedMeanValues;
		return this;
	}

	/**
	 * @param keyedStdDevValues
	 * @return
	 */
	public Expression withKeyedStdDevValues(Map<String, Double> keyedStdDevValues) {
		this.keyedStdDevValues = keyedStdDevValues;
		return this;
	}

	/**
	 * @param child
	 */
	public void addChild(Expression child) {
		children.add(child);
		if(!isMultiOperand() && children.size() > 2) {
			BasicUtils.assertFail("too many operandss");
		}
	}
	
	/**
	 * @param child
	 */
	public void removeChild(Expression child) {
		children.remove(child);
	}

	/**
	 * @param name
	 * @return
	 */
	protected BaseAttribute getAttribute(String name) {
		if (null == attributes) {
			throw new IllegalStateException("missing attribute schena");
		}
		BaseAttribute foundAttr = null;
		for (BaseAttribute attr : attributes) {
			if (name.equals(attr.getName())) {
				foundAttr = attr;
				break;
			}
		}
		return foundAttr;
	}
	
	/**
	 * @return
	 */
	public  Object evaluate() {
		return children.get(0).evaluate();
	}
	
	/**
	 * @return
	 */
	public  int getPrecedence() {
		return ROOT_PREC;
	}

	/**
	 * @return
	 */
	public boolean isMultiOperand() {
		return false;
	}
}
