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

/**
 * @author pranab
 *
 */
public class AttributeFilter {
	private List<AttributePredicate> predicates = new ArrayList<AttributePredicate>();

	/**
	 * @param filter
	 */
	public AttributeFilter(String filter) {
		AttributePredicate  predicate = null;
		String[] preds = filter.split(",");
		for (String pred : preds) {
			String[] predParts = pred.split("\\s+");
			int attr = Integer.parseInt(predParts[0]);
			String[] valueParts  = predParts[2].split(":");
			
			if (valueParts[0].equals("int")) {
				int iValue = Integer.parseInt(valueParts[1]);
				predicate = new IntAttributePredicate(attr, predParts[1], iValue);
			} else if (valueParts[0].equals("double")) {
				double dValue = Double.parseDouble(valueParts[1]);
				predicate = new DoubleAttributePredicate(attr, predParts[1], dValue);
			} else if (valueParts[0].equals("string")) {
				predicate = new StringAttributePredicate(attr, predParts[1], valueParts[1]);
			} else {
				throw new IllegalArgumentException("invalid data type");
			}
			predicates.add(predicate);
		}
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
				break;
			}
		}
		
		return status;
	}
}
