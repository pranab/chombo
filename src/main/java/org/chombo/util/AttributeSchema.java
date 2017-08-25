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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * @author pranab
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttributeSchema<T extends BaseAttribute>  implements Serializable {
	private static final long serialVersionUID = -6568183248952599026L;
	protected List<T> attributes;

	public List<T> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<T> attributes) {
		this.attributes = attributes;
	}
	
	public int getAttributeCount() {
		return attributes.size();
	}
	
	/**
	 * @return
	 */
	public int[] getAttributeOrdinals() {
		int[] ordinals = new int[attributes.size()];
		int i = 0;
		for (T attr :  attributes) {
			ordinals[i++] = attr.getOrdinal();
		}
		
		return ordinals;
	}
	
	/**
	 * @param ordinal
	 * @return
	 */
	public T findAttributeByOrdinal(int ordinal) {
		T attribute = null;
		for (T attr :  attributes) {
			if (attr.getOrdinal() == ordinal) {
				attribute = attr;
				break;
			}
		}
		
		return attribute;
	}

	/**
	 * @param ordinal
	 * @return
	 */
	public T findAttributeByName(String name) {
		T attribute = null;
		for (T attr :  attributes) {
			if (name.equals(attr.getName())) {
				attribute = attr;
				break;
			}
		}
		
		return attribute;
	}

	/**
	 * @param ordinals
	 * @return
	 */
	public List<T> findAttributesByOrdinals(int[] ordinals) {
		List<T> attributes = new ArrayList<T>();
		for (int ordinal : ordinals ) {
			for (T attr :  attributes) {
				if (attr.getOrdinal() == ordinal) {
					attributes.add(attr);
				}
			}
		}
		return attributes;
	}

	/**
	 * @param index
	 * @return
	 */
	public T findAttribute(int index) {
		return attributes.get(index);
	}
	
	/**
	 * @param includeTypes
	 * @return
	 */
	public List<T> getAttributes(String... includeTypes ) {
		List<T> filtAttributes  = new ArrayList<T>() ;
		for (T attr : attributes) {
			String type = attr.getDataType();
			for (String includeType : includeTypes) {
				if (includeType.equals(type)) {
					filtAttributes.add(attr);
					break;
				}
			}
		}
		return filtAttributes;
	}
	

}
