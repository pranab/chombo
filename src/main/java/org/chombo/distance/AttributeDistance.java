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

import org.chombo.util.BaseAttribute;

/**
 * Meta data related to inter attribute distance
 * @author pranab
 *
 */
public class AttributeDistance extends BaseAttribute{
	private String algorithm;
	private double weight;
	private double upperThreshold;
	private double lowerThreshold;
	private int setting;
	
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
		setting = setting | 8;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
		setting = setting | 1;
	}
	public double getUpperThreshold() {
		return upperThreshold;
	}
	public void setUpperThreshold(double upperThreshold) {
		this.upperThreshold = upperThreshold;
		setting = setting | 2;
	}
	public double getLowerThreshold() {
		return lowerThreshold;
	}
	public void setLowerThreshold(double lowerThreshold) {
		this.lowerThreshold = lowerThreshold;
		setting = setting | 4;
	}
	
	public boolean isWeightSet() {
		return (setting & 1) == 1;
	}

	public boolean isUpperThresholdSet() {
		return (setting & 2) == 1;
	}
	
	public boolean isLowerThresholdSet() {
		return (setting & 4) == 1;
	}

	public boolean isAlgorithmSet() {
		return (setting & 8) == 1;
	}
	
	
}
