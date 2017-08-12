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
 * Schema related to data validation and transformation
 * @author pranab
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessorAttributeSchema extends AttributeSchema<ProcessorAttribute> {
	private List<ProcessorAttribute> attributeGenerators;
	private List<String> rowValidators = new ArrayList<String>();

	/**
	 * @return
	 */
	public List<ProcessorAttribute> getAttributeGenerators() {
		return attributeGenerators;
	}

	/**
	 * @param attributeGenerators
	 */
	public void setAttributeGenerators(List<ProcessorAttribute> attributeGenerators) {
		this.attributeGenerators = attributeGenerators;
	}
	
	public List<String> getRowValidators() {
		return rowValidators;
	}

	public void setRowValidators(List<String> rowValidators) {
		this.rowValidators = rowValidators;
	}

	/**
	 * gets dervived attribute counts from tramsformers and generators
	 * @return
	 */
	public int findDerivedAttributeCount() {
		int count = 0;
		
		//derived attributes from transformers
		for (ProcessorAttribute attr :  attributes) {
			if (null != attr.getTargetFieldOrdinals()) { 
				count += attr.getTargetFieldOrdinals().length;
			}
		}

		//generated attributes
		if (null != attributeGenerators) {
			for (ProcessorAttribute genAttr :  attributeGenerators) {
				count += genAttr.getTargetFieldOrdinals().length;
			}
		}
		
		return count;
	}
	
	/**
	 * 
	 */
	public void validateTargetAttributeMapping() {
		int count = findDerivedAttributeCount();
		int[] targetOrdinals = new int[count];
		for (int i = 0; i <  targetOrdinals.length; ++i) {
			targetOrdinals[i] = -1;
		}
		
		//transformation
		validateTargetAttributeMappingHelper(attributes, targetOrdinals );
		
		//generation
		if (null != attributeGenerators) {
			validateTargetAttributeMappingHelper(attributeGenerators, targetOrdinals );
		}
		
		//check if non mapped attribute
		int nonMappedCount = 0;
		for (int i = 0; i <  targetOrdinals.length; ++i) {
			if (targetOrdinals[i] == -1) {
				++nonMappedCount;
			}
		}
		
		if (nonMappedCount > 0) {
			throw new  IllegalArgumentException("found " + nonMappedCount +  "  target field ordinals");
		}
		
	}
	
	/**
	 * @param attributes
	 * @param targetOrdinals
	 */
	private void validateTargetAttributeMappingHelper(List<ProcessorAttribute> attributes, int[] targetOrdinals ) {
		for (ProcessorAttribute attr :  attributes) {
			int[] targets = attr.getTargetFieldOrdinals();
			if (null != targets) {
				for (int i = 0; i < targets.length; ++i) {
					int targetOrd = targets[i];
					if (targetOrdinals[targetOrd] == -1) {
						targetOrdinals[targetOrd] = targetOrd;
					} else {
						throw new  IllegalArgumentException("multiple mapping for target field ordinal");
					}
				}
			}
		}
	}
	
	
}
