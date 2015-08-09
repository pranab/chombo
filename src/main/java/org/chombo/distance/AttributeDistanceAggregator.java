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

/**
 * Metadata related to attribute aggregation
 * @author pranab
 *
 */
public class AttributeDistanceAggregator {
	private int[] ordinals;
	private String algorithm = "euclidean";
	private double weight = 1.0;
	private double param;
	
	public int[] getOrdinals() {
		return ordinals;
	}
	public void setOrdinals(int[] ordinals) {
		this.ordinals = ordinals;
	}
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public double getParam() {
		return param;
	}
	public void setParam(double param) {
		this.param = param;
	}
	
}
