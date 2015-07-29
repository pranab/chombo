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

import java.util.Map;

import org.chombo.util.Attribute;
import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public abstract class Validator {
	protected String tag;
	protected int ordinal;
	protected Attribute attribute;
	protected Map<String,String> configParams;
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 */
	public Validator(String tag, int ordinal, AttributeSchema<Attribute> schema) {
		super();
		this.tag = tag;
		this.ordinal = ordinal;
		attribute = schema.findAttributeByOrdinal(ordinal);
	}
	
	public abstract boolean isValid(String value);

	public String getTag() {
		return tag;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public Map<String, String> getConfigParams() {
		return configParams;
	}

	public void setConfigParams(Map<String, String> configParams) {
		this.configParams = configParams;
	}
	
}
