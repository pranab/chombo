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

package org.chombo.custom;

import java.util.Map;

import org.chombo.util.BasePredicate;
import org.chombo.util.BasicUtils;

/**
 * Specified fields can not be empty
 * @author pranab
 *
 */
public class NoMissingFieldFilter extends BasePredicate {
	private int[] fieldOrdinals;
	private static final String fieldOrdinalsLabel = "pro.filter.udf.max.missing.field.ordinals";

	/**
	 * 
	 */
	public NoMissingFieldFilter() {
	}
	
	/**
	 * @param context
	 */
	public NoMissingFieldFilter(Map<String, Object> context) {
		super(context);
		fieldOrdinals = BasicUtils.intArrayFromString((String)context.get(fieldOrdinalsLabel));
	}

	@Override
	public BasePredicate withContext(Map<String, Object> context) {
		super.withContext(context);
		fieldOrdinals = BasicUtils.intArrayFromString((String)context.get(fieldOrdinalsLabel));
		return this;
	}
	
	@Override
	public void build(String predicate) {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean evaluate(String[] record) {
		boolean valid = true;
		for (int i : fieldOrdinals) {
			if (BasicUtils.isBlank(record[i])) {
				valid = false;
				break;
			}
		}
		return valid;
	}
	
	@Override
	public boolean evaluate(String field) {
		return true;
	}

	@Override
	public void build(int attribute, String operator, String value) {
		// TODO Auto-generated method stub
	}

}
