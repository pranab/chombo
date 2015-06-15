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

/**
 * Base class for attribute. Used in schema definition. Initialized based on schema definition
 * JSON file
 * @author pranab
 *
 */
public class Attribute extends BaseAttribute{
	protected boolean id;
	protected boolean classAttribute;
	protected List<String> cardinality;
	protected double  min;
	protected boolean minDefined;
	protected double  max;
	protected boolean maxDefined;
	protected double mean;
	protected boolean meanDefined;
	protected double variance;
	protected double stdDev;
	protected boolean stdDevDefined;
	protected double skew;
	protected boolean skewDefined;
	protected double maxZscore;
	protected String datePattern;
	protected boolean nullable;
	protected String stringPattern;
	protected String minString;
	protected String maxString;
	
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
		minDefined = true;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
		maxDefined = true;
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
		meanDefined = true;
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
		stdDevDefined = true;
	}
	public double getSkew() {
		return skew;
	}
	public void setSkew(double skew) {
		this.skew = skew;
		skewDefined = true;
	}
	public double getMaxZscore() {
		return maxZscore;
	}
	public void setMaxZscore(double maxZscore) {
		this.maxZscore = maxZscore;
	}
	public String getDatePattern() {
		return datePattern;
	}
	public void setDatePattern(String datePattern) {
		this.datePattern = datePattern;
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

	public boolean isMinDefined() {
		return minDefined;
	}
	public boolean isMaxDefined() {
		return maxDefined;
	}
	public boolean isMeanDefined() {
		return meanDefined;
	}
	public boolean isStdDevDefined() {
		return stdDevDefined;
	}
	public boolean isSkewDefined() {
		return skewDefined;
	}
	public int cardinalityIndex(String value) {
		return cardinality.indexOf(value);
	}
}
