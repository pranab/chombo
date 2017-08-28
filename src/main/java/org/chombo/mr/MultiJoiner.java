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

package org.chombo.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.AttributeFilter;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Joins multiple data sets
 * @author pranab
 *
 */
public class MultiJoiner extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MultiJoiner  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(MultiJoiner.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        
        job.setMapperClass(MultiJoiner.JoinerMapper.class);
        job.setReducerClass(MultiJoiner.JoinerReducer.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        int numReducer = job.getConfiguration().getInt("muj.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class JoinerMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] splitPrefixes;
		private List<int[]>  keyFields = new ArrayList<int[]>();
        private List<int[]> projectedFields = new ArrayList<int[]>();
        private AttributeFilter[] attrFilters;
        private int currentSplit;
        private AttributeFilter currentFilter;
        private int[] currentKeyFields;
        private int[] currentProjectedFields;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	splitPrefixes = Utility.assertStringArrayConfigParam(config, "muj.split.prefixes", 
        			Utility.configDelim,"missing split file prefix list");
        	
        	//key fields and projected fields
        	for(String prefix : splitPrefixes) {
        		keyFields.add(Utility.intArrayFromString(config.get("muj.key.fields." + prefix),Utility.configDelim));
        		projectedFields.add(Utility.intArrayFromString(config.get("muj.projected.fields." + prefix),Utility.configDelim));
        	}
        	
        	//filters and current split
        	attrFilters = new AttributeFilter[splitPrefixes.length];
        	String splitName = ((FileSplit)context.getInputSplit()).getPath().getName();
        	for (int i = 0; i < splitPrefixes.length; ++i) {
        		String filtExp = config.get("muj.filter." + splitPrefixes[i]);
        		attrFilters[i] =  null != filtExp ? new AttributeFilter(filtExp) : null;
        		
        		if (splitName.startsWith(splitPrefixes[i])) {
        			currentSplit = i;
        		}
        	}
        	
        	currentKeyFields = keyFields.get(currentSplit);
        	currentProjectedFields = projectedFields.get(currentSplit);
            currentFilter = attrFilters[currentSplit];
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            if (null == currentFilter || currentFilter.evaluate(items)) {
            	Utility.createStringTuple(items, currentKeyFields, outKey);
            	outKey.add(currentSplit);
            	
            	Utility.createStringTuple(items, currentProjectedFields, outVal);
            	outVal.prepend(currentSplit);
            	
            	context.write(outKey, outVal);
            }
        }        
	}
	
    /**
     * @author pranab
     *
     */
    public static class JoinerReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
		private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
		private Map<Integer, List<Tuple>> projectedFields = new HashMap<Integer, List<Tuple>>();
		private String[] splitPrefixes;
		private int numSplits;
		private boolean outerJoin;
		private boolean missingJoin;
		private int[] tupleIndexes;
		private int[] tupleListSizes;
		private int[] tupleSizes;
		private List<Tuple> tuples;
		private Tuple tuple;
		private boolean tupleTraversalDone;
		
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	splitPrefixes = Utility.assertStringArrayConfigParam(config, "muj.split.prefixes", 
        			Utility.configDelim,"missing split file prefix list");
        	numSplits = splitPrefixes.length;
        	outerJoin = config.getBoolean("muj.outer.join", false);
        	tupleIndexes = new int[numSplits];
        	
        	
        	//key fields and projected fields
        	for (int i = 0; i < numSplits; ++i) {
        		String prefix = splitPrefixes[i];
        		tupleSizes[i] = Utility.intArrayFromString(config.get("muj.projected.fields." + prefix), 
        				Utility.configDelim).length;
        	}
        	
        }
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	key.setDelim(fieldDelimOut);
        	projectedFields.clear();
        	int lastSplitIndex = 0;
        	missingJoin  = false;
        	
        	for (Tuple value : values){
        		int splitIndex = value.getInt(0);
        		tuples = projectedFields.get(splitIndex);
        		if (null == tuples) {
        			if (splitIndex > lastSplitIndex + 1) {
        				//happens when there is no match from 1 or more splits
        				for (int cur = lastSplitIndex + 1; cur < splitIndex; ++ cur) {
        					//for outer join adding dummy tuple helps when there is missing tuple because join fails                 			tuples = new ArrayList<Tuple>();
                			tuples.add(createDummyTuple(tupleSizes[cur]));
                			projectedFields.put(splitIndex, tuples);
                			missingJoin = true;
        				}
        			}
        			tuples = new ArrayList<Tuple>();
        			projectedFields.put(splitIndex, tuples);
        			lastSplitIndex = splitIndex;
        		}
        		tuples.add(value.beginningSubTuple(1));
        	}
       
        	//emit joined tuples
        	if (outerJoin || !outerJoin && !missingJoin) {
            	for (int i = 0; i < numSplits; ++i) {
            		tupleIndexes[i] = 0;
            		tupleListSizes[i] = projectedFields.get(i).size();
            	}
            	
            	//traverse all levels 
            	tupleTraversalDone = false;
        		while(!tupleTraversalDone) {
                	stBld.delete(0, stBld.length());
                	stBld.append(key.toStringEnd(key.getSize()-1));
                	//nested join
                	for (int i = 0; i < numSplits; ++i) {
                		//tuple list for a split and the a tuple within list
                		tuples = projectedFields.get(i);
                		tuple = tuples.get(tupleIndexes[i]);
                		stBld.append(fieldDelimOut).append(tuple.toString());
                	}  
                	outVal.set(stBld.toString());
                	context.write(NullWritable.get(), outVal);
                	
                	updateTupleIndexes();
        		}
        	} 
        	
        }
        
        /**
         * 
         */
        private void updateTupleIndexes() {
        	for (int i = numSplits -1; i >= 0; --i) {
        		if (tupleIndexes[i] < tupleListSizes[i] -1) {
        			tupleIndexes[i] += 1;
        			break;
        		} else {
        			//reset to zero and increment one level up
        			tupleIndexes[i] = 0;
        			
        			if (i == 0) {
        				//we are at top level and done
        				tupleTraversalDone = true;
        			}
        		}
        	}
        }
        
        /**
         * @param size
         * @return
         */
        private Tuple createDummyTuple(int size) {
        	Tuple dummy = new Tuple();
        	for (int i = 0; i < size; ++i) {
        		dummy.add("");
        	}
        	return dummy;
        }
    }	
    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiJoiner(), args);
        System.exit(exitCode);
	}
   
}
