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

import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 */
public abstract class BaseAttributeFilter {
	protected Map<String, Object> context;
	protected GenericAttributeSchema schema;
	protected List<? extends BaseAttribute> attributes;
	
	/**
	 * 
	 */
	public BaseAttributeFilter() {
	}

	/**
	 * @param context
	 */
	public BaseAttributeFilter(Map<String, Object> context) {
		this.context = context;
	}

	/**
	 * @param context
	 * @return
	 */
	public BaseAttributeFilter withContext(Map<String, Object> context){
		this.context = context;
		return this;
	}
	
	/**
	 * @param schema
	 * @return
	 */
	public BaseAttributeFilter withAttributes(List<? extends BaseAttribute> attributes){
		this.attributes = attributes;
		return this;
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
	 * @param filter
	 */
	public abstract  void build(String filter);
	
	/**
	 * @param filter
	 */
	public abstract boolean evaluate(String[] record);

}
