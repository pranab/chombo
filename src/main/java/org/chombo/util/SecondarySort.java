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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author pranab
 *
 */
public class SecondarySort {
    /**
     * @author pranab
     *
     */
    public static class TextIntIdPairPartitioner extends Partitioner<TextInt, Text> {
	     @Override
	     public int getPartition(TextInt key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.baseHashCode() % numPartitions;
	     }
   }
 
    /**
     * @author pranab
     *
     */
    public static class TextIntIdPairTuplePartitioner extends Partitioner<TextInt, Tuple> {
	     @Override
	     public int getPartition(TextInt key, Tuple value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.baseHashCode() % numPartitions;
	     }
   
   }
    
    
    /**
     * @author pranab
     *
     */
    public static class TupleTextPartitioner extends Partitioner<Tuple, Text> {
	     @Override
	     public int getPartition(Tuple key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.hashCodeBase() % numPartitions;
	     }
   }
   
    /**
     * @author pranab
     *
     */
    public static class TupleIntPartitioner extends Partitioner<Tuple, IntWritable> {
	     @Override
	     public int getPartition(Tuple key, IntWritable value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.hashCodeBase() % numPartitions;
	     }
   }

    /**
     * @author pranab
     *
     */
    public static class TuplePairPartitioner extends Partitioner<Tuple, Tuple> {
	     @Override
	     public int getPartition(Tuple key, Tuple value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.hashCodeBase() % numPartitions;
	     }
   }
   
    
    /**
     * @author pranab
     *
     */
    public static class TextIntIdPairGroupComprator extends WritableComparator {
    	protected TextIntIdPairGroupComprator() {
    		super(TextInt.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		TextInt t1 = (TextInt)w1;
    		TextInt t2 = (TextInt)w2;
    		
    		int comp = t1.baseCompareTo(t2);
    		return comp;
    	}
     }

    
    /**
     * @author pranab
     *
     */
    public static class TuplePairGroupComprator extends WritableComparator {
    	protected TuplePairGroupComprator() {
    		super(Tuple.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		Tuple t1 = (Tuple)w1;
    		Tuple t2 = (Tuple)w2;
    		
    		int comp =t1.compareToBase(t2);
    		return comp;
    	}
     }
   
    /**
     * @author pranab
     *
     */
    public static class RawIntKeyTextPartitioner extends Partitioner<IntWritable, Text> {
	     @Override
	     public int getPartition(IntWritable key, Text value, int numPartitions) {
	    	 //key value is the partition
		     return key.get();
	     }
    }
    
}
