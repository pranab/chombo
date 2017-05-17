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

import java.util.Collections;
import java.util.List;

/**
 * @author pranab
 *
 */
public class CategoricalSet implements Domain {
	private List<String> values;
	private String delim;
	
	/**
	 * @param value
	 * @param delim
	 */
	public CategoricalSet(String value, String delim) {
		String[] items = value.split(delim);
		values = BasicUtils.generateUniqueList(items);
		this.delim = delim;
	}

	/* (non-Javadoc)
	 * @see org.chombo.util.Domain#add(java.lang.String)
	 */
	@Override
	public void add(String value) {
		BasicUtils.addToUniqueList(values, value);
	}

	/* (non-Javadoc)
	 * @see org.chombo.util.Domain#isContained(java.lang.String)
	 */
	@Override
	public boolean isContained(String value) {
		return values.contains(value);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		Collections.sort(values);
		return BasicUtils.join(values, delim);
	}

}
