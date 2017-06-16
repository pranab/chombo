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

import java.util.List;

/**
 * Sampler with distribution based on rank 
 * @author pranab
 *
 * @param <T>
 */
public class RankedItemsRejectionSampler<T> extends  NonParametricDistrRejectionSampler<T> {

	/**
	 * @param items
	 * @param decayRate
	 */
	public RankedItemsRejectionSampler(List<T> items, double decayRate) {
		for (int i = 0; i < items.size(); ++i) {
			double rank = i + 1;
			double distr = 1.0  / Math.pow(rank, decayRate);
			add(items.get(i), distr);
		}
		normalize();
	}
	
	/**
	 * @param items
	 * @param decayRate
	 */
	public RankedItemsRejectionSampler(T[] items, double decayRate) {
		for (int i = 0; i < items.length; ++i) {
			double rank = i + 1;
			double distr = 1.0  / Math.pow(rank, decayRate);
			add(items[i], distr);
		}
		normalize();
	}
}
