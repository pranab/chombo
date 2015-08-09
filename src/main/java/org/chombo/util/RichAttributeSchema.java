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

/**
 * @author pranab
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RichAttributeSchema  extends AttributeSchema<RichAttribute> {

	/**
	 * @return
	 */
	public List<RichAttribute> getFields() {
		return getAttributes();
	}

	
	/**
	 * @return
	 */
	public RichAttribute getIdField() {
		RichAttribute idField = null;
		for (RichAttribute field : attributes) {
			if (field.isId()) {
				idField = field;
			}
		}
		return idField;
	}
	
	/**
	 * @return
	 */
	public List<RichAttribute> getIdFields() {
		List<RichAttribute> idFields = new ArrayList<RichAttribute>();
		for (RichAttribute field : attributes) {
			if (field.isId()) {
				idFields.add(field);
			}
		}
		return idFields;
	}

	/**
	 * @return
	 */
	public RichAttribute getPartitionField() {
		RichAttribute partitionField = null;
		for (RichAttribute field : attributes) {
			if (field.isPartitionAttribute()) {
				partitionField = field;
			}
		}
		return partitionField;
	}
	
	/**
	 * @return
	 */
	public List<RichAttribute> getPartitionFields() {
		List<RichAttribute> partitionFields = new ArrayList<RichAttribute>();
		for (RichAttribute field : attributes) {
			if (field.isPartitionAttribute()) {
				partitionFields.add(field);
			}
		}
		return partitionFields;
	}

	/**
	 * @param skipId
	 * @param skipPartition
	 * @return
	 */
	public int getAttributeCount(boolean skipId, boolean skipPartition) {
		int count = 0;
		for (RichAttribute attr : attributes) {
			if (!(attr.isId() && skipId || attr.isPartitionAttribute() && skipPartition))
				++count;
		}
		return count;
	}
	
	/**
	 * @return
	 */
	public int[] getNumericAttributeOrdinals() {
		int[] ordinals = null;
		List<Integer> ordinalList = new ArrayList<Integer>();
		
		for (RichAttribute attr : attributes) {
			if (attr.isInteger() || attr.isDouble()) {
				ordinalList.add(attr.ordinal);
			}
		}
		ordinals = new int[ordinalList.size()];
		for (int i = 0; i < ordinalList.size(); ++i) {
			ordinals[i] = ordinalList.get(i);
		}
		
		return ordinals;
	}
}
