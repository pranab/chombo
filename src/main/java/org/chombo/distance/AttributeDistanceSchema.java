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


package org.chombo.distance;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pranab
 *
 */
public class AttributeDistanceSchema implements Serializable {
	private AttributeDistanceAggregator[] attrAggregators;
	private AttributeDistance[] attrDistances;
	private Map<Integer, AttributeDistance> attributeDistanceByOrdinal = new HashMap<Integer, AttributeDistance>();
	
	/**
	 * @return
	 */
	public AttributeDistanceAggregator[] getAttrAggregators() {
		return attrAggregators;
	}
	
	/**
	 * @param attrAggregators
	 */
	public void setAttrAggregators(AttributeDistanceAggregator[] attrAggregators) {
		this.attrAggregators = attrAggregators;
	}
	
	/**
	 * @return
	 */
	public AttributeDistance[] getAttrDistances() {
		return attrDistances;
	}
	
	/**
	 * @param attrDistances
	 */
	public void setAttrDistances(AttributeDistance[] attrDistances) {
		this.attrDistances = attrDistances;
	}
	
	/**
	 * @param ordinal
	 * @return
	 */
	public AttributeDistance findAttributeDistanceByOrdinal(int ordinal) {
		AttributeDistance attrDist = attributeDistanceByOrdinal.get(ordinal);
		if (null == attrDist) {
			for (AttributeDistance thisAttrDist : attrDistances) {
				if (thisAttrDist.getOrdinal() == ordinal) {
					attrDist = thisAttrDist;
					attributeDistanceByOrdinal.put(ordinal, attrDist);
				}
			}
		}
		return attrDist;
	}
	
	
}
