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

public class ProcessorAttribute extends BaseAttribute {
	private String normalizerStrategy;
	private List<String> validators = new ArrayList<String>();
	
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
	
	
}
