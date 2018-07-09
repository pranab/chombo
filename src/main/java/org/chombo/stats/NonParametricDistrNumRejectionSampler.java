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

package org.chombo.stats;

import java.util.ArrayList;
import java.util.Collections;

import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;

/** 
 * Nor parametric rejection sampler for numeric base
 * @author pranab
 *
 */
public class NonParametricDistrNumRejectionSampler extends NonParametricDistrRejectionSampler<Double> {
	private double minBase;
	private double maxBase;
	
	/**
	 * @return
	 */
	public Double sample() {
		double sampled;
		if (!normalized) {
			normalize();
		}
		if (null == values) {
			values = new ArrayList<Double>(normDistr.keySet());
			Collections.sort(values);
			minBase = values.get(0);
			maxBase = values.get(values.size() - 1);
		}
		
		while(true) {
			//select base randomly and find distr
			sampled  = minBase + Math.random() * (maxBase - minBase);
			Pair<Double, Double> bounds = BasicUtils.getBoundingValues(values, sampled);
			double leftDistr = normDistr.get(bounds.getLeft());
			double rightDistr = normDistr.get(bounds.getRight());
			double sampDistr = leftDistr + (rightDistr - leftDistr) / (bounds.getRight() - bounds.getLeft()) * 
					(sampled -  bounds.getLeft());
			
			//if a random number falls below distr then select the item and return
			if (Math.random() < sampDistr) {
				break;
			}
		}
		return sampled;
	}

}
