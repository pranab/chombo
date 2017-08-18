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

import java.util.Map;

/**
 * @author pranab
 *
 */
public abstract class BasePredicate {
	protected int attribute;
	protected String operator;
	protected Map<String, Object> context;
	
	/**
	 * 
	 */
	public BasePredicate(){
	}

	/**
	 * @param attribute
	 * @param operator
	 */
	public BasePredicate(int attribute, String operator){
		this.attribute = attribute;
		this.operator = operator;
	}

	/**
	 * @param context
	 */
	public BasePredicate(Map<String, Object> context){
		this.context = context;
	}
	
	/**
	 * @param predicate
	 */
	public abstract void build(String predicate);
	
	/**
	 * @param context
	 * @return
	 */
	public BasePredicate withContext(Map<String, Object> context) {
		this.context = context;
		return this;
	}

	/**
	 * @param attribute
	 * @param operator
	 * @param value
	 */
	public abstract void build(int attribute, String operator, String value);

	/**
	 * evaluates predicate 
	 * @param record
	 * @return
	 */
	public abstract boolean evaluate(String[] record);
	
	/**
	 * evaluates predicate 
	 * @param field
	 * @return
	 */
	public abstract boolean evaluate(String field);
	

}
