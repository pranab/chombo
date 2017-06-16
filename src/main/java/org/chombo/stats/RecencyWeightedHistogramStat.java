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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Maintains a  list of histogram based on sliding interval. Active histogram is based on recent 
*  windowSize worth of data
 * @author pranab
 *
 */
public class RecencyWeightedHistogramStat extends HistogramStat {
	private int windowSize;
	private int slidingInterval;
	private List<BackupHistogramStat> backupHistStats = new ArrayList<BackupHistogramStat>();
	
	/**
	 * @param binWidth
	 * @param windowSize
	 * @param slidingInterval
	 */
	public RecencyWeightedHistogramStat(int binWidth,  int windowSize, int slidingInterval ) {
		super(binWidth);
		if (windowSize % slidingInterval != 0) {
			throw new IllegalArgumentException("window size should be a multiple of sliding interval");
		}
		this.windowSize = windowSize;
		this.slidingInterval = slidingInterval;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.util.HistogramStat#add(int, int)
	 */
	public void add(int value, int count) {
		super.add(value, count);
		
		//add another backup histogram
		if (sampleCount % slidingInterval == 0) {
			backupHistStats.add(new BackupHistogramStat() );
		}
		
		//update all backup histogram
		for (BackupHistogramStat histStat : backupHistStats) {
			histStat. add(value,  count);
		}
		
		//switch active histogram 
		if (sampleCount % windowSize == 0) {
			BackupHistogramStat histStat = backupHistStats.remove(0);
			this.binMap = histStat.binMap;
			this.count =  histStat.count;
			this.sum  = histStat.sum;
			sampleCount = 0;
		}		
	}
	
	public class BackupHistogramStat {
		private Map<Integer, HistogramStat.Bin> binMap = new HashMap<Integer, HistogramStat.Bin>();
		private int count;
		private double sum = 0.0;
		
		/**
		 * @param value
		 * @param count
		 */
		public void add(int value, int count) {
			int index = (int)(value / binWidth);
			Bin bin = binMap.get(index);
			if (null == bin) {
				bin = new Bin(index);
				binMap.put(index, bin);
			}
			bin.addCount(count);
			this.count += count;
			sum += value * count;
		}
		
	}

}
