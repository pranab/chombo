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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 */
public class AttributeFilter {
	private List<AttributePredicate> predicates = new ArrayList<AttributePredicate>();
	private Map<String, Object> context;
	public static final String COND_SEP = ",";
	private static String condSeparator;
	
	public AttributeFilter(){
	}

	/**
	 * @param filter
	 */
	public AttributeFilter(String filter) {
		build(filter);
	}
	
	/**
	 * @param filter
	 */
	public void build(String filter) {
		AttributePredicate  predicate = null;
		String[] preds = filter.split(getCondSeparator());
		for (String pred : preds) {
			String[] predParts = pred.trim().split(AttributePredicate.PREDICATE_SEP);
			int attr = Integer.parseInt(predParts[0]);
			String[] valueParts  = predParts[2].split(AttributePredicate.DATA_TYPE_SEP);
			
			if (valueParts[0].equals(BaseAttribute.DATA_TYPE_INT)) {
				predicate = new IntAttributePredicate(attr, predParts[1], valueParts[1]);
			} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
				predicate = new DoubleAttributePredicate(attr, predParts[1], valueParts[1]);
			} else if (valueParts[0].equals(BaseAttribute.DATA_TYPE_STRING)) {
				if (null != context) {
					predicate = new StringAttributePredicate();
					predicate.withContext(context).build(attr, predParts[1], valueParts[1]);
				} else {
					predicate = new StringAttributePredicate(attr, predParts[1], valueParts[1]);
				}
			} else {
				throw new IllegalArgumentException("invalid data type");
			}
			predicates.add(predicate);
		}
	}
	
	public AttributeFilter withContext(Map<String, Object> context) {
		this.context = context;
		return this;
	}
	
	/**
	 * @param record
	 * @return
	 */
	public boolean evaluate(String[] record) {
		boolean status = true;
		for (AttributePredicate  predicate : predicates) {
			status = status & predicate.evaluate(record);
			if (!status) {
				//predicates and connected
				break;
			}
		}
		
		return status;
	}
	
	public static String getCondSeparator() {
		return condSeparator != null ? condSeparator : COND_SEP;
	}

	public static void setCondSeparator(String condSeparator) {
		AttributeFilter.condSeparator = condSeparator;
	}
	
}
