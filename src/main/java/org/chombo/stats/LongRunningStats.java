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
 * Running stats for a field
 * @author pranab
 *
 */
public class LongRunningStats {
	private int field;
	private long count;
	private long sum;
	private long sumSq;
	private long avg;
	private double stdDev;
	
	public LongRunningStats(int field, long count, long sum, long sumSq) {
		super();
		this.field = field;
		this.count = count;
		this.sum = sum;
		this.sumSq = sumSq;
	}
	
	public LongRunningStats(int field, long avg, double stdDev) {
		super();
		this.field = field;
		this.avg = avg;
		this.stdDev = stdDev;
	}

	public void accumulate(long count, long sum, long sumSq) {
		this.count += count;
		this.sum += sum;
		this.sumSq += sumSq;
	}
	
	public void process() {
		avg =  sum / count;
		if (1 == count) {
			stdDev = 0;
		} else {
			double ave = (double)sum / count;
			stdDev = (double)sumSq / count -  ave * ave;
			stdDev = Math.sqrt(stdDev);
		}
	}

	public int getField() {
		return field;
	}

	public long getCount() {
		return count;
	}

	public long getSum() {
		return sum;
	}

	public long getSumSq() {
		return sumSq;
	}

	public long getAvg() {
		return avg;
	}

	public double getStdDev() {
		return stdDev;
	}
	
	public void incrementCount() {
		++count;
	}
}
