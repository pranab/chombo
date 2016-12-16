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
	private double maxGeoDistance = -1.0;
	private String textSimilarityStrategy = "jaccard";
	private double jaccardSrcNonMatchingTermWeight = 1.0;
	private double jaccardTrgNonMatchingTermWeight = 1.0;
	
	/**
	 * @return
	 */
	public String getAlgorithm() {
		return algorithm;
	}
	
	/**
	 * @param algorithm
	 */
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
		setting = setting | 8;
	}
	
	/**
	 * @return
	 */
	public double getWeight() {
		return weight;
	}
	
	/**
	 * @param weight
	 */
	public void setWeight(double weight) {
		this.weight = weight;
		setting = setting | 1;
	}
	
	/**
	 * @return
	 */
	public double getUpperThreshold() {
		return upperThreshold;
	}
	
	/**
	 * @param upperThreshold
	 */
	public void setUpperThreshold(double upperThreshold) {
		this.upperThreshold = upperThreshold;
		setting = setting | 2;
	}
	
	/**
	 * @return
	 */
	public double getLowerThreshold() {
		return lowerThreshold;
	}
	
	/**
	 * @param lowerThreshold
	 */
	public void setLowerThreshold(double lowerThreshold) {
		this.lowerThreshold = lowerThreshold;
		setting = setting | 4;
	}
	
	/**
	 * @return
	 */
	public double getMaxGeoDistance() {
		return maxGeoDistance;
	}

	/**
	 * @param maxGeoDistance
	 */
	public void setMaxGeoDistance(double maxGeoDistance) {
		this.maxGeoDistance = maxGeoDistance;
		setting = setting | 16;
	}

	/**
	 * @return
	 */
	public String getTextSimilarityStrategy() {
		return textSimilarityStrategy;
	}
	
	/**
	 * @param textSimilarityStrategy
	 */
	public void setTextSimilarityStrategy(String textSimilarityStrategy) {
		this.textSimilarityStrategy = textSimilarityStrategy;
		setting = setting | 16;
	}

	/**
	 * @return
	 */
	public double getJaccardSrcNonMatchingTermWeight() {
		return jaccardSrcNonMatchingTermWeight;
	}

	/**
	 * @param jaccardSrcNonMatchingTermWeight
	 */
	public void setJaccardSrcNonMatchingTermWeight(double jaccardSrcNonMatchingTermWeight) {
		this.jaccardSrcNonMatchingTermWeight = jaccardSrcNonMatchingTermWeight;
	}

	/**
	 * @return
	 */
	public double getJaccardTrgNonMatchingTermWeight() {
		return jaccardTrgNonMatchingTermWeight;
	}

	/**
	 * @param jaccardTrgNonMatchingTermWeight
	 */
	public void setJaccardTrgNonMatchingTermWeight(
			double jaccardTrgNonMatchingTermWeight) {
		this.jaccardTrgNonMatchingTermWeight = jaccardTrgNonMatchingTermWeight;
	}

	/**
	 * @return
	 */
	public boolean isWeightSet() {
		return (setting & 1) == 1;
	}

	/**
	 * @return
	 */
	public boolean isUpperThresholdSet() {
		return (setting & 2) == 1;
	}
	
	/**
	 * @return
	 */
	public boolean isLowerThresholdSet() {
		return (setting & 4) == 1;
	}

	/**
	 * @return
	 */
	public boolean isAlgorithmSet() {
		return (setting & 8) == 1;
	}
	
	/**
	 * @return
	 */
	public boolean isMaxGeoDistanceSet() {
		return (setting & 16) == 1;
	}
	
	/**
	 * @return
	 */
	public boolean isTextSimilarityStrategySet() {
		return (setting & 32) == 1;
	}
	
}
