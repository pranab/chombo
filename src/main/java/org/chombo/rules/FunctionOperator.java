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

import java.util.List;

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class FunctionOperator extends Operator {
	private String name;
	private List<String> args;

	/**
	 * @param root
	 * @param parent
	 * @param token
	 */
	public FunctionOperator(Expression root, Expression parent, String token) {
		super(root,  parent,  token);
	}

	public FunctionOperator withName(String name) {
		this.name = name;
		return this;
	}

	public FunctionOperator withArgs(List<String> args) {
		this.args = args;
		return this;
	}

	@Override
	public Object evaluate() {
		if (name.equals("mean")) {
			double mean = getMean();
			value = mean;
		} else if (name.equals("stdDev")) {
			double mean = getStdDev();
			value = mean;
		} else {
			BasicUtils.assertFail("unsupported function");
		}

		return value;
	}
	
	@Override
	public int getPrecedence() {
		return TERM_PREC;
	}
	
	/**
	 * @return
	 */
	private double getMean() {
		BaseAttribute attr = root.getAttribute(args.get(0));
		BasicUtils.assertNotNull(attr, "undefined variable " + token);
		int fieldOrd = attr.getOrdinal();
		String key = root.inputKey + "," + fieldOrd;
		return root.keyedMeanValues.get(key);
	}

	/**
	 * @return
	 */
	private double getStdDev() {
		BaseAttribute attr = root.getAttribute(args.get(0));
		BasicUtils.assertNotNull(attr, "undefined variable " + token);
		int fieldOrd = attr.getOrdinal();
		String key = root.inputKey + "," + fieldOrd;
		return root.keyedStdDevValues.get(key);
	}
}
