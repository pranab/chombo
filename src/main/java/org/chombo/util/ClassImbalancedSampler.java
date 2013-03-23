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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Sampler for class imbalanced data. Makes data set class balanced by sub sampling
 * majority classess
 * @author pranab
 *
 */
public class ClassImbalancedSampler {
	private int imbalanceRatioCalInterval = 100;
	private int counter = 0;
	private Random randomGenerator = new Random();
	private Map<String,ClassAttrubuteStat> classAttrStats = new HashMap<String,ClassAttrubuteStat>();
	
	public ClassImbalancedSampler(int imbalanceRatioCalInterval) {
		this.imbalanceRatioCalInterval = imbalanceRatioCalInterval;
	}
	
	public boolean next(String classAttrVal) {
		boolean sampleIt = false;
		if (++counter % imbalanceRatioCalInterval == 0) {
			calculateImbalanceRatio();
		}
		ClassAttrubuteStat classAttrStat = classAttrStats.get(classAttrVal);
		if (null == classAttrStat) {
			classAttrStat = new ClassAttrubuteStat();
			classAttrStats.put(classAttrVal, classAttrStat);
		}
		classAttrStat.incrCount();
		
		sampleIt = counter < imbalanceRatioCalInterval ? true : 
			(randomGenerator.nextInt(100) < classAttrStat.getImbalanceRatio());
		if (sampleIt) {
			classAttrStat.incrSampleCount();
		}
		return sampleIt;
	}
	
	private void calculateImbalanceRatio() {
		int min = Integer.MAX_VALUE;
		for (String clAttrVal : classAttrStats.keySet()) {
			ClassAttrubuteStat clAttrStat = classAttrStats.get(clAttrVal);
			if (null != clAttrStat && clAttrStat.getCount() < min) {
				min = clAttrStat.getCount();
			}
		}

		int ratio;
		for (String clAttrVal : classAttrStats.keySet()) {
			ClassAttrubuteStat clAttrStat = classAttrStats.get(clAttrVal);
			if (null != clAttrStat) {
				ratio = (100 * min) / clAttrStat.getCount();
				clAttrStat.setImbalanceRatio(ratio);
			}
		}	
	}
	
	private static class ClassAttrubuteStat {
		private int count;
		private int imbalanceRatio;
		private int sampledCount;
		
		public int getCount() {
			return count;
		}

		public int getImbalanceRatio() {
			return imbalanceRatio;
		}

		public void setImbalanceRatio(int imbalanceRatio) {
			this.imbalanceRatio = imbalanceRatio;
		}

		public int getSampledCount() {
			return sampledCount;
		}

		public void incrCount()  {
			++count;
		}
		
		public void incrSampleCount() {
			++sampledCount;
		}
		
	}
	
 }
