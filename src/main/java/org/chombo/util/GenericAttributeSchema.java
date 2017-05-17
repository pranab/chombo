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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GenericAttributeSchema extends AttributeSchema<Attribute>{

	/**
	 * @param includeTypes
	 * @return
	 */
	public List<Attribute> getQuantAttributes(String... includeTypes ) {
		List<Attribute> filtAttributes  = new ArrayList<Attribute>() ;
		for (Attribute attr : attributes) {
			String type = attr.getDataType();
			for (String includeType : includeTypes) {
				if (includeType.equals(type) && !attr.isId() && !attr.isPartitionAttribute() ) {
					filtAttributes.add(attr);
					break;
				}
			}
		}
		return filtAttributes;
	}

	/**
	 * @param attributes
	 * @return
	 */
	public boolean areNumericalAttributes(int...  attributes) {
		boolean valid = true;
		for (int attr : attributes) {
			Attribute attrMeta = findAttributeByOrdinal(attr);
			if (!attrMeta.isNumerical()) {
				valid = false;
				break;
			}
		}
		return valid;
	}

	/**
	 * @param attributes
	 * @return
	 */
	public boolean areCategoricalAttributes(int...  attributes) {
		boolean valid = true;
		for (int attr : attributes) {
			Attribute attrMeta = findAttributeByOrdinal(attr);
			if (!attrMeta.isCategorical()) {
				valid = false;
				break;
			}
		}
		return valid;
	}

	/**
	 * @param attributes
	 * @return
	 */
	public boolean areStringAttributes(int...  attributes) {
		boolean valid = true;
		for (int attr : attributes) {
			Attribute attrMeta = findAttributeByOrdinal(attr);
			if (!attrMeta.isString()) {
				valid = false;
				break;
			}
		}
		return valid;
	}
	
	/**
	 * @return
	 */
	public Attribute getIdField() {
		Attribute idField = null;
		for (Attribute field : attributes) {
			if (field.isId()) {
				idField = field;
			}
		}
		return idField;
	}

	/**
	 * @return
	 */
	public List<Attribute> getIdFields() {
		List<Attribute> idFields = new ArrayList<Attribute>();
		for (Attribute field : attributes) {
			if (field.isId()) {
				idFields.add(field);
			}
		}
		return idFields;
	}

	/**
	 * @return
	 */
	public Attribute getPartitionField() {
		Attribute partitionField = null;
		for (Attribute field : attributes) {
			if (field.isPartitionAttribute()) {
				partitionField = field;
			}
		}
		return partitionField;
	}
	
	/**
	 * @return
	 */
	public List<Attribute> getPartitionFields() {
		List<Attribute> partitionFields = new ArrayList<Attribute>();
		for (Attribute field : attributes) {
			if (field.isPartitionAttribute()) {
				partitionFields.add(field);
			}
		}
		return partitionFields;
	}
	
}
