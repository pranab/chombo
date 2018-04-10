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

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Base class for attribute. Used in schema definition. Initialized based on schema definition
 * JSON file
 * @author pranab
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Attribute extends BaseAttribute{
	protected boolean partitionAttribute;
	protected boolean id;
	protected boolean classAttribute;
	protected List<String> cardinality;
	protected double  min;
	protected double  max;
	protected double mean;
	protected double variance;
	protected double stdDev;
	protected double skew;
	protected double maxZscore;
	protected boolean nullable;
	protected String stringPattern;
	protected String stringPatternName;
	protected String minString;
	protected String maxString;
	protected int length;
	protected String earliestDate;
	protected String latestDate;
	private int settings;
	
	
	public boolean isPartitionAttribute() {
		return partitionAttribute;
	}
	public void setPartitionAttribute(boolean partitionAttribute) {
		this.partitionAttribute = partitionAttribute;
	}
	public boolean isId() {
		return id;
	}
	public void setId(boolean id) {
		this.id = id;
	}
	public boolean isClassAttribute() {
		return classAttribute;
	}
	public void setClassAttribute(boolean classAttribute) {
		this.classAttribute = classAttribute;
	}	
	public double getMin() {
		return min;
	}
	public void setMin(double min) {
		this.min = min;
		settings = settings | 1;
	}
	public double getMinLength() {
		return min;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
		settings = settings | 2;
	}
	public double getMaxLength() {
		return max;
	}
	public List<String> getCardinality() {
		return cardinality;
	}
	public void setCardinality(List<String> cardinality) {
		this.cardinality = cardinality;
	}
	public double getMean() {
		return mean;
	}
	public void setMean(double mean) {
		this.mean = mean;
		settings = settings | 4;
	}
	public double getVariance() {
		return variance;
	}
	public void setVariance(double variance) {
		this.variance = variance;
	}
	public double getStdDev() {
		return stdDev;
	}
	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
		settings = settings | 8;
	}
	public double getSkew() {
		return skew;
	}
	public void setSkew(double skew) {
		this.skew = skew;
		settings = settings | 16;
	}
	public double getMaxZscore() {
		return maxZscore;
	}
	public void setMaxZscore(double maxZscore) {
		this.maxZscore = maxZscore;
	}
	public boolean isNullable() {
		return nullable;
	}
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	public String getStringPattern() {
		return stringPattern;
	}
	public void setStringPattern(String stringPattern) {
		this.stringPattern = stringPattern;
	}
	public String getStringPatternName() {
		return stringPatternName;
	}
	public void setStringPatternName(String stringPatternName) {
		this.stringPatternName = stringPatternName;
	}
	public String getMinString() {
		return minString;
	}
	public void setMinString(String minString) {
		this.minString = minString;
	}
	public String getMaxString() {
		return maxString;
	}
	public void setMaxString(String maxString) {
		this.maxString = maxString;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public String getEarliestDate() {
		return earliestDate;
	}
	public void setEarliestDate(String earliestDate) {
		this.earliestDate = earliestDate;
	}
	public String getLatestDate() {
		return latestDate;
	}
	public void setLatestDate(String latestDate) {
		this.latestDate = latestDate;
	}
	public boolean isMinDefined() {
		return (settings & 1) == 1;
	}
	public boolean isMaxDefined() {
		return (settings & 2) == 2;
	}
	public boolean isMeanDefined() {
		return (settings & 4) ==4;	
	}
	public boolean isStdDevDefined() {
		return (settings & 8) == 8;	
	}
	public boolean isSkewDefined() {
		return (settings & 16) == 16;	
	}
	public int cardinalityIndex(String value) {
		return cardinality.indexOf(value);
	}
}
