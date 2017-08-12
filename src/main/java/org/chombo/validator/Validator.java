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

import java.io.Serializable;
import java.util.Map;

import org.chombo.util.Attribute;
import org.chombo.util.AttributeSchema;
import org.chombo.util.ProcessorAttribute;

/**
 * @author pranab
 *
 */
public abstract class Validator  implements Serializable {
	private static final long serialVersionUID = -4336547628640729070L;
	protected String tag;
	protected Map<String,String> configParams;
	protected String fieldDelim = ",";
	protected ProcessorAttribute prAttr;
	
	/**
	 * @param tag
	 * @param ordinal
	 * @param schema
	 */
	public Validator(String tag, ProcessorAttribute prAttr) {
		super();
		this.tag = tag;
		this.prAttr = prAttr;
	}
	
	/**
	 * @param tag
	 */
	public Validator(String tag) {
		super();
		this.tag = tag;
	}
	
	public abstract boolean isValid(String value);

	public String getTag() {
		return tag;
	}

	public int getOrdinal() {
		return prAttr.getOrdinal();
	}

	public Map<String, String> getConfigParams() {
		return configParams;
	}

	public void setConfigParams(Map<String, String> configParams) {
		this.configParams = configParams;
	}

	public String getFieldDelim() {
		return fieldDelim;
	}

	public void setFieldDelim(String fieldDelim) {
		this.fieldDelim = fieldDelim;
	}
	
}
