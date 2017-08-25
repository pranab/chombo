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
import java.util.List;

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
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Perform set operation like intersection and union between 2 keyed sets, Maintains list
 * like order
 * @author pranab
 *
 */
public class SetOperator extends Configured implements Tool {
	private static String OP_INTERSECT = "intersect";
	private static String OP_UNION = "union";
	private static String OP_FIRST_MINUS_SECOND = "firstMinusSecond";
	private static String OP_SECOND_MINUS_FIRST = "secondMinusFirst";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "SetMerger  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SetOperator.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        
        job.setMapperClass(SetOperator.MergerMapper.class);
        job.setReducerClass(SetOperator.MergerReducer.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("seo.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class MergerMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private boolean isFirstTypeSplit;
        private int subKey;
        private int[]  keyFieldFirst;
        private int[]  keyFieldSecond;
        private int[]  valFieldFirst;
        private int[]  valFieldSecond;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String firstTypePrefix = config.get("seo.first.type.prefix", "first");
        	isFirstTypeSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(firstTypePrefix);
        	subKey = isFirstTypeSplit ? 0 : 1;
        	keyFieldFirst = Utility.assertIntArrayConfigParam(config, "seo.key.field.first", Utility.DEF_FIELD_DELIM, 
        			"missing key field ordinal for first dataset");
        	keyFieldSecond = Utility.assertIntArrayConfigParam(config, "seo.key.field.second", Utility.DEF_FIELD_DELIM, 
        			"missing key field ordinal for second dataset");
        	valFieldFirst = Utility.intArrayFromString(config, "seo.value.field.first");
        	valFieldSecond = Utility.intArrayFromString(config, "seo.value.field.second");
        }   
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            //key
            if (isFirstTypeSplit) {
            	Utility.createStringTuple(items, keyFieldFirst, outKey, true);
        	} else {
            	Utility.createStringTuple(items, keyFieldSecond, outKey, true);
        	}
            outKey.add(subKey);
            
            //value
            String rec = null;
            if (null != valFieldFirst && null != valFieldSecond) {
            	//selected fields
            	rec = isFirstTypeSplit ? BasicUtils.extractFields(items, valFieldFirst, BasicUtils.configDelim) :
            		BasicUtils.extractFields(items, valFieldSecond, BasicUtils.configDelim);
            } else {
            	//full record
            	rec = value.toString();
            }
            outVal.initialize();
            outVal.add(subKey, rec);
            
            context.write(outKey, outVal);
        }        
	}

    /**
     * @author pranab
     *
     */
    public static class MergerReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
		private String fieldDelimOut;
		private String operation;
		private boolean orderByFirst;
		private Pair<List<String>, List<String>> records = new Pair<List<String>, List<String>>(
				new ArrayList<String>(), new ArrayList<String>());
		private List<String> result = new ArrayList<String>();
		private List<String> primary;
		private List<String> secondary;
		private int retainCount;
		private List<String> reatinedResult;
		private boolean outputFormatCompact;
		private boolean matchByKey;
		private int subKey;
		private boolean outputKey;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	operation = Utility.assertStringConfigParam(config, "seo.merge.operation", "missing set merge operation");
        	orderByFirst = config.getBoolean("seo.order.by.first.set", true);
        	retainCount = config.getInt("seo.retain.count", -1);
        	outputFormatCompact = config.getBoolean("seo.output.format.compact", false);
        	matchByKey = config.getBoolean("seo.mtach.by.key", true);
        	boolean outputKey = config.getBoolean("seo.output.key", false);
        }	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	result.clear();
        	records.getLeft().clear();
        	records.getRight().clear();
        	for (Tuple value : values){
        		subKey = value.getInt(0);
        		if (subKey == 0) {
        			records.getLeft().add(value.getString(1));
        		} else {
        			records.getRight().add(value.getString(1));
        		}
        	}

        	//primary and secondary list
    		if(orderByFirst) {
    			primary = records.getLeft();
    			secondary = records.getRight();
    		} else {
    			primary = records.getRight();
    			secondary = records.getLeft();
    		}     
    		
    		//set operation
        	if (matchByKey) {
        		//maximum one record per key from each data set
        		matchByKey();
        	} else {
        		//any number of records per partition from each data set
        		matchByPartition();
        	}
        	
        	//emit
        	String keyStr = null;
        	if (outputKey) {
        		key.setDelim(fieldDelimOut);
        		keyStr = key.toStringEnd(key.getSize()-1);
        	}
        	if (!reatinedResult.isEmpty()) {
	        	if (outputFormatCompact) {
	        		if (outputKey) {
	        			outVal.set(keyStr + fieldDelimOut + BasicUtils.join(reatinedResult, fieldDelimOut));
	        		} else {
	        			outVal.set(BasicUtils.join(reatinedResult, fieldDelimOut));
	        		}
	    			context.write(NullWritable.get(), outVal);
	        	} else {
	        		for (String val : reatinedResult) {
	        			if (outputKey) {
	        				outVal.set(keyStr + fieldDelimOut + val);
	        			} else {
	        				outVal.set(val);
	        			}
	        			context.write(NullWritable.get(), outVal);
	        		}
	        	}
        	}
        }
        
        /**
         * 
         */
        private void matchByPartition() {
    		//operation
        	if (operation.equals(OP_INTERSECT) ) {
        		//intersection
        		for (String val : primary) {
        			if (secondary.contains(val)) {
        				result.add(val);
        			}
        		}
        		//truncate by retain count
        		reatinedResult = retainCount > 0 ? result.subList(0, retainCount) : result;
        	} else if (operation.equals(OP_FIRST_MINUS_SECOND) ) {
        		//first minus second
        		for (String val : primary) {
        			if (!secondary.contains(val)) {
        				result.add(val);
        			}
        		}
        		//truncate by retain count
        		reatinedResult = retainCount > 0 ? result.subList(0, retainCount) : result;
        	} else if (operation.equals(OP_SECOND_MINUS_FIRST) ) {
        		//second minus first
        		for (String val : secondary) {
        			if (!primary.contains(val)) {
        				result.add(val);
        			}
        		}
        		//truncate by retain count
        		reatinedResult = retainCount > 0 ? result.subList(0, retainCount) : result;
        	} else if (operation.equals(OP_UNION)) {
        		if (retainCount > 0) {
        			//retail half of retain count form each list
        			result.addAll(primary.subList(0, retainCount/2));
        			result.addAll(secondary.subList(0, retainCount/2));
        			reatinedResult = result;
        		} else {
        			//retail all form each list
        			result.addAll(primary);
        			result.addAll(secondary);
        			reatinedResult = result;
        		}
        	} else {
        		throw new IllegalStateException("invalid set operation");
        	}
       	
        }
        
        /**
         * 
         */
        private void matchByKey() {
    		//operation
        	if (operation.equals(OP_INTERSECT) ) {
        		//intersection
        		if (primary.size() == 1 && secondary.size() == 1) {
        			result.add(primary.get(0));
        		}
        	} else if (operation.equals(OP_FIRST_MINUS_SECOND) ) {
        		//first minus second
        		if (primary.size() == 1 && secondary.size() == 0) {
        			result.add(primary.get(0));
        		}
        	} else if (operation.equals(OP_SECOND_MINUS_FIRST) ) {
        		//second minus first
        		if (primary.size() == 0 && secondary.size() == 1) {
        			result.add(secondary.get(0));
        		}
        	} else if (operation.equals(OP_UNION)) {
    			result.addAll(primary);
    			result.addAll(secondary);
        	} else {
        		throw new IllegalStateException("invalid set operation");
        	}
    		//truncate by retain count
    		reatinedResult = result;
        }
        
    }
    
    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SetOperator(), args);
        System.exit(exitCode);
	}
    
}
