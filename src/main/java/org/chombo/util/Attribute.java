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
 * @author pranab
 *
 */
public class Attribute {
	protected String name;
	protected int ordinal = -1;
	protected boolean id;
	protected String dataType;
	protected List<String> cardinality;
	protected int min;
	protected int max;
	protected double mean;
	protected double variance;
	protected double stdDev;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinal() {
		return ordinal;
	}
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}
	public boolean isId() {
		return id;
	}
	public void setId(boolean id) {
		this.id = id;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
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
	}
	public boolean isCategorical() {
		return dataType.equals("categorical");
	}

	public boolean isInteger() {
		return dataType.equals("int");
	}

	public boolean isDouble() {
		return dataType.equals("double");
	}

	public boolean isText() {
		return dataType.equals("text");
	}
	
	public int cardinalityIndex(String value) {
		return cardinality.indexOf(value);
	}
}
