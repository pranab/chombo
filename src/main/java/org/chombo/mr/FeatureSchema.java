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

import java.util.List;

/**
 * @author pranab
 *
 */
public class FeatureSchema {
	private List<FeatureField> fields;

	public List<FeatureField> getFields() {
		return fields;
	}

	public void setFields(List<FeatureField> fields) {
		this.fields = fields;
	}
	
	public FeatureField findFieldByOrdinal(int ordinal) {
		FeatureField selField = null;
		for (FeatureField field : fields) {
			if (field.getOrdinal() == ordinal) {
				selField = field;
				break;
			}
		}
		return selField;
	}

}
