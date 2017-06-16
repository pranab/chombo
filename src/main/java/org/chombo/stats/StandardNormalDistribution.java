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


/**
 * Standard normal distribution
 * @author pranab
 *
 */
public class StandardNormalDistribution implements ProbabilityDistribution {
	private double mean;
	private double stdDev;
	private double[] distr = 
		{0.5000, 0.5398, 0.5793, 0.6179, 0.6554, 0.6915, 0.7257, 0.7580, 0.7881, 0.8159, 0.8413, 0.8643,
		 0.8849, 0.9032, 0.9192, 0.9332, 0.9452, 0.9554, 0.9641, 0.9713, 0.9773, 0.9821, 0.9861, 0.9893,
		 0.9918, 0.9938, 0.9953, 0.9974, 0.9981, 0.9987, 0.9990, 0.9993, 0.9995, 0.9997, 0.9998, 0.9999};
	private double step = 0.1;
	
	/**
	 * @param mean
	 * @param stdDev
	 */
	public StandardNormalDistribution(double mean, double stdDev) {
		super();
		this.mean = mean;
		this.stdDev = stdDev;
	}

	/* (non-Javadoc)
	 * @see org.chombo.util.ProbabilityDistribution#getDistr(double)
	 */
	public double getDistr(double ord) {
		//normalize
		ord = (ord - mean) / stdDev;
		
		double di = -1.0;
		double ordReqd = ord < 0 ? -ord : ord;
		double ordStep = 0;
		for (int i = 0; i < distr.length; ++i, ordStep += step) {
			if (ordStep > ordReqd) {
				di = distr[i-1] + (distr[i] - distr[i-1]) * (ordReqd -  (ordStep - step)) / step;
				break;
			}
		}
		//not within lookup range
		di = di < 0 ? 1.0 : di;
		
		//negative ord
		di = ord < 0 ? 1.0 - di : di;

		return di;
	}
}
