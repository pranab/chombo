
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
 * @author pranab
 *
 */
public class MaxMissingFieldFilter extends BasePredicate {
	private int maxMissingFields;
	private static final String maxMissingFieldsLabel = "pro.filter.udf.max.missing.fields";
	
	public MaxMissingFieldFilter() {
	}
	
	/**
	 * @param context
	 */
	public MaxMissingFieldFilter(Map<String, Object> context) {
		super(context);
		maxMissingFields = BasicUtils.asInt((String)context.get(maxMissingFieldsLabel));
	}

	@Override
	public BasePredicate withContext(Map<String, Object> context) {
		super.withContext(context);
		maxMissingFields = BasicUtils.asInt((String)context.get(maxMissingFieldsLabel));
		return this;
	}

	@Override
	public boolean evaluate(String[] record) {
		int count = BasicUtils.missingFieldCount(record);
		return count <= maxMissingFields;
	}

	@Override
	public void build(String filter) {
		// TODO Auto-generated method stub
	}

	@Override
	public void build(int attribute, String operator, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean evaluate(String field) {
		return true;
	}

}
