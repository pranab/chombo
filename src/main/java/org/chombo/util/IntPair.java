/*
 * Sifarish: Recommendation Engine
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author pranab
 *
 */
public class IntPair implements WritableComparable<IntPair>  {
	private IntWritable first;
	private IntWritable second;
	
	public IntPair() {
		first = new IntWritable();
		second =  new IntWritable();
	}
	
	public void set(int first, int second) {
		this.first.set(first);
		this.second.set(second);
	}
	
	public IntWritable getFirst() {
		return first;
	}
	public void setFirst(IntWritable first) {
		this.first = first;
	}
	public IntWritable getSecond() {
		return second;
	}
	public void setSecond(IntWritable second) {
		this.second = second;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}
	@Override
	public int compareTo(IntPair tiPair) {
		int cmp = first.compareTo(tiPair.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(tiPair.getSecond());
		}
		return cmp;
	}
	
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	public int baseHashCode() {
		return first.hashCode();
	}
	
	public boolean equals(Object obj) {
		boolean isEqual =  false;
		if (obj instanceof IntPair) {
			IntPair iPair = (IntPair)obj;
			isEqual = first.equals(iPair.first) && second.equals(iPair.second);
		}
		
		return isEqual;
	}
	
	public String toString() {
		return "" + first.get() + ":" + second.get();
	}

}
