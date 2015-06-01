
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

package org.chombo.validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvalidData {
	private String record;
	private Map<Integer, List<String>> invalidFields  = new HashMap<Integer, List<String>>();
	
	/**
	 * @param record
	 */
	public InvalidData(String record) {
		super();
		this.record = record;
	}
	
	/**
	 * @param ordinal
	 * @param validationType
	 */
	public void addValidationFailure(int ordinal, String validationType) {
		List<String> validationTypes = invalidFields.get(ordinal);
		if (null == validationTypes) {
			validationTypes = new ArrayList<String>();
			invalidFields.put(ordinal, validationTypes);
		}
		validationTypes.add(validationType);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuilder stBld = new StringBuilder();
		stBld.append(record).append("\n");
		for (int ord : invalidFields.keySet()) {
			stBld.append("field:" + ord).append("\n");
			for (String valType : invalidFields.get(ord)) {
				stBld.append(valType).append("  ");
			}
			stBld.append("\n");
		}
		
		return stBld.toString();
	}

}
