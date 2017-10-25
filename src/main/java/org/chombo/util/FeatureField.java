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
 * Field for feature attribute and class attribute
 * @author pranab
 *
 */
public class FeatureField extends RichAttribute implements  Comparable<FeatureField>{
	protected boolean  feature;
	protected int maxSplit;
	protected double  splitScanInterval;
	protected int maxCatAttrSplitGroups;

	public boolean isFeature() {
		return feature;
	}

	public void setFeature(boolean feature) {
		this.feature = feature;
	}

	public int getMaxSplit() {
		return maxSplit;
	}

	public void setMaxSplit(int maxSplit) {
		this.maxSplit = maxSplit;
	}


	public double getSplitScanInterval() {
		return splitScanInterval;
	}

	public void setSplitScanInterval(double splitScanInterval) {
		this.splitScanInterval = splitScanInterval;
	}

	public int getMaxCatAttrSplitGroups() {
		return maxCatAttrSplitGroups;
	}

	public void setMaxCatAttrSplitGroups(int maxCatAttrSplitGroups) {
		this.maxCatAttrSplitGroups = maxCatAttrSplitGroups;
	}

	@Override
	public int compareTo(FeatureField that) {
		int ret = this.ordinal < that.ordinal ? -1 : (this.ordinal == that.ordinal? 0 : 1);
		return ret;
	}

}
