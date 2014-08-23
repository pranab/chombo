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
 * @author pranab
 *
 */
public class FeatureCount  {
	private int ordinal;
	private String type;
	private List<BinCount> counts = new ArrayList<BinCount>();
	private double laplaceProb;
	private long mean;
	private long stdDev;
	boolean parametric = false;
	
	/**
	 * @param ordinal
	 * @param type
	 */
	public FeatureCount( int ordinal, String type) {
		super();
		this.ordinal = ordinal;
		this.type = type;
	}

	/**
	 * @return
	 */
	public int getOrdinal() {
		return ordinal;
	}

	/**
	 * @param ordinal
	 */
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}

	/**
	 * @return
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type
	 */
	public void setType(String type) {
		this.type = type;
	}	

	/**
	 * @return
	 */
	public List<BinCount> getCounts() {
		return counts;
	}

	/**
	 * @param counts
	 */
	public void setCounts(List<BinCount> counts) {
		this.counts = counts;
	}

	/**
	 * @param binCount
	 */
	public void addBinCount(BinCount binCount) {
		boolean added = false;
		for (BinCount thisBinCount : counts) {
			if (thisBinCount.getBin().equals(binCount.getBin())){
				thisBinCount.addCount(binCount.getCount());
				added = true;
			}
		}
		
		if (!added){
			counts.add(binCount);
		}
	}
	
	/**
	 * @param mean
	 * @param stdDev
	 */
	public void setDistrParameters(long mean, long stdDev) {
		this.mean = mean;
		this.stdDev = stdDev;
		parametric = true;
	}
	
	/**
	 * @param total
	 */
	public void normalize(int total) {
		if (!parametric) {
			for (BinCount binCount : counts) {
				binCount.normalize(total);
			}
			laplaceProb = 1.0 / (1 + total);
		}
	}	
	
	/**
	 * @param bin
	 * @return
	 */
	public double getProb(String bin) {
		double prob = laplaceProb;
		for (BinCount binCount : counts) {
			if (binCount.getBin().equals(bin)) {
				prob = binCount.getProb();
				break;
			}
		}	
		return prob;
	}

	/**
	 * @param val
	 * @return
	 */
	public double getProb(int val) {
		double prob = 1.0 / (Math.sqrt(2.0 * Math.PI) * stdDev);
		prob *= Math.exp(-((val - mean) * (val - mean)) / (2.0 * stdDev * stdDev));
		return prob;
	}
}
