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

package org.chombo.mr;

import java.util.ArrayList;
import java.util.List;

import org.chombo.util.AttributeSchema;

/**
 * @author pranab
 *
 */
public class HistogramSchema  extends AttributeSchema<HistogramField> {

	/**
	 * @return
	 */
	public List<HistogramField> getFields() {
		return getAttributes();
	}

	
	/**
	 * @return
	 */
	public HistogramField getIdField() {
		HistogramField idField = null;
		for (HistogramField field : attributes) {
			if (field.isId()) {
				idField = field;
			}
		}
		return idField;
	}
	
	/**
	 * @return
	 */
	public List<HistogramField> getIdFields() {
		List<HistogramField> idFields = new ArrayList<HistogramField>();
		for (HistogramField field : attributes) {
			if (field.isId()) {
				idFields.add(field);
			}
		}
		return idFields;
	}

	/**
	 * @return
	 */
	public HistogramField getPartitionField() {
		HistogramField partitionField = null;
		for (HistogramField field : attributes) {
			if (field.isPartitionAttribute()) {
				partitionField = field;
			}
		}
		return partitionField;
	}
	
	/**
	 * @return
	 */
	public List<HistogramField> getPartitionFields() {
		List<HistogramField> partitionFields = new ArrayList<HistogramField>();
		for (HistogramField field : attributes) {
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
		for (HistogramField attr : attributes) {
			if (!(attr.isId() && skipId || attr.isPartitionAttribute() && skipPartition))
				++count;
		}
		return count;
	}
	
}
