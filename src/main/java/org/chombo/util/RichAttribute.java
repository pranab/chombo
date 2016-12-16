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


/**
 * Derived class for attributes that require histogram
 * @author pranab
 *
 */
public class RichAttribute  extends Attribute {
	protected int bucketWidth;
	protected boolean bucketWidthDefined;
	protected int numBuckets;
	protected boolean numBucketsDefined;
	private double[] bucketBoundaries;

	/**
	 * @return
	 */
	public int getBucketWidth() {
		return bucketWidth;
	}

	/**
	 * @param bucketWidth
	 */
	public void setBucketWidth(int bucketWidth) {
		this.bucketWidth = bucketWidth;
		bucketWidthDefined = true;
	}

	/**
	 * @return
	 */
	public boolean isBucketWidthDefined() {
		return bucketWidthDefined;
	}
	
	/**
	 * @return
	 */
	public int getNumBuckets() {
		return numBuckets;
	}

	/**
	 * @param numBuckets
	 */
	public void setNumBuckets(int numBuckets) {
		this.numBuckets = numBuckets;
		numBucketsDefined = true;
	}
	
	/**
	 * @param value
	 * @return
	 */
	public int getBucket(String value) {
		int bucket = 0;
		double dValue = Double.parseDouble(value);
		if (bucketWidthDefined) {
			if (numBucketsDefined) {
				//symmetric wrt to mean
				if (null == bucketBoundaries) {
					if (!isMeanDefined() || !isStdDevDefined()) {
						throw new IllegalArgumentException("when bucket count and width are set, mean and std dev are required");
					}
					double width = bucketWidth *  stdDev / 100.0;
					double offset = mean - 0.5 * numBuckets * width;
					intializeBuckets(offset, width);
				}
				bucket =  bucketIndex(dValue);
			} else {
				//simple index
				bucket = (int)(dValue / bucketWidth);
			}
		} else {
			if (numBucketsDefined) {
				//divide range by bucket count
				if (null == bucketBoundaries) {
					if (!isMinDefined() || !isMaxDefined()) {
						throw new IllegalArgumentException("when only bucket count is set, min and max are required");
					}
					double offset = min;
					double width =  (max - min) / numBuckets;
					intializeBuckets(offset, width);
				}
				bucket =  bucketIndex(dValue);
			} else { 
				//assume integer with small range
				if (isInteger()) {
					bucket = Integer.parseInt(value);
				} else {
					throw new IllegalArgumentException("Either bucket count or bucket width should be defined for double attribute");
				}
			}
		}
		return bucket;
	}
	

	/**
	 * @param offset
	 * @param width
	 */
	private void intializeBuckets(double offset, double width) {
		bucketBoundaries = new double[numBuckets + 1];
		for (int i = 0; i < numBuckets + 1; ++i, offset += width){
			bucketBoundaries[i] = offset;
		}
	}
	
	/**
	 * @param value
	 * @return
	 */
	private int bucketIndex(double value) {
		int bucket = 0;
		for (int i = 1; i <  bucketBoundaries.length && value > bucketBoundaries[i]; ++i, ++bucket) {
		}
		return bucket;
	}
}
