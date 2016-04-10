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

/**
 * Various meta data related to processing an attribute for validation and transformation
 * @author pranab
 *
 */
public class ProcessorAttribute extends Attribute {
	private String normalizerStrategy;
	private List<String> validators;
	private List<String> transformers;
	private int[] targetFieldOrdinals;
	private double buckeWidth;
	private BinaryCategoryCreator binaryCategoryCreator;
	
	
	public static final String NORMALIZER_ZSCORE = "zScore";
	public static final String NORMALIZER_MIN_MAX = "minMax";
	
	public String getNormalizerStrategy() {
		return normalizerStrategy;
	}
	public void setNormalizerStrategy(String normalizerStrategy) {
		this.normalizerStrategy = normalizerStrategy;
	}
	public List<String> getValidators() {
		return validators;
	}
	public void setValidators(List<String> validators) {
		this.validators = validators;
	}
	public List<String> getTransformers() {
		return transformers;
	}
	public void setTransformers(List<String> transformers) {
		this.transformers = transformers;
	}
	public int[] getTargetFieldOrdinals() {
		return targetFieldOrdinals;
	}
	public void setTargetFieldOrdinals(int[] targetFieldOrdinals) {
		this.targetFieldOrdinals = targetFieldOrdinals;
	}
	public double getBuckeWidth() {
		return buckeWidth;
	}
	public void setBuckeWidth(double buckeWidth) {
		this.buckeWidth = buckeWidth;
	}
	public BinaryCategoryCreator getBinaryCategoryCreator() {
		return binaryCategoryCreator;
	}
	public void setBinaryCategoryCreator(BinaryCategoryCreator binaryCategoryCreator) {
		this.binaryCategoryCreator = binaryCategoryCreator;
	}
	
	
}
