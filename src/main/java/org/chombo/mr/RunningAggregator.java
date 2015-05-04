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
import java.util.HashMap;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.LongRunningStats;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;


/**
 * Calculates running average and other stats of some quantity
 * @author pranab
 *
 */
public class RunningAggregator  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Running aggregates  for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(RunningAggregator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(RunningAggregator.AggrMapper.class);
        job.setReducerClass(RunningAggregator.AggrReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("rug.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class AggrMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] quantityAttrOrdinals;
        private boolean isAggrFileSplit;
        private int[] idFieldOrdinals;
        private long newValue;
        private int statOrd;
        private static final int PER_FIELD_STAT_VAR_COUNT = 6;
        private static final Logger LOG = Logger.getLogger(RunningAggregator.AggrMapper.class);

        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }

        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String value = Utility.assertConfigParam( config, "quantity.attr.ordinals", "quantity field ordinals must be provided");
        	quantityAttrOrdinals = Utility.intArrayFromString(value);
        	
        	String aggrFilePrefix = config.get("aggregate.file.prefix", "");
        	if (!aggrFilePrefix.isEmpty()) {
        		isAggrFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(aggrFilePrefix);
        	} else {
            	String incrFilePrefix =Utility.assertConfigParam( config, "incremental.file.prefix", "Incremental file prefix needs to be specified");
            	isAggrFileSplit = !((FileSplit)context.getInputSplit()).getPath().getName().startsWith(incrFilePrefix);
        	}
        	
        	value = Utility.assertConfigParam( config, "id.field.ordinals", "ID field ordinals must be provided");
        	idFieldOrdinals = Utility.intArrayFromString(value);
       }
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	outKey.initialize();
        	outVal.initialize();
        	
        	if (isAggrFileSplit) {
        		for (int i = 0; i < idFieldOrdinals.length; ++i) {
      				outKey.append(items[i]);
        		}
    			statOrd = idFieldOrdinals.length;
    			for ( int ord : quantityAttrOrdinals) {
        			//existing aggregation - quantity attrubute ordinal, count, sum, sum of squares
                    outVal.add(0, Integer.parseInt(items[statOrd]), Long.parseLong(items[statOrd+1]) ,  
                    		Long.parseLong(items[statOrd + 2]),  Long.parseLong(items[statOrd + 3]));
                    statOrd += PER_FIELD_STAT_VAR_COUNT;
    			}
        	} else {
          		for (int ord : idFieldOrdinals ) {
          			outKey.append(items[ord]);
          		}
            	
            	//incremental - first run will have only incremental file
    			for ( int ord : quantityAttrOrdinals) {
	        		newValue = Long.parseLong( items[ord]);
	                outVal.add(1,ord, (long)1, newValue, newValue * newValue);
    			}
        	}
        	context.write(outKey, outVal);
        }
 	}	

	   /**
  * @author pranab
  *
  */
 public static class AggrReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		private Text outVal = new Text();
 		private StringBuilder stBld =  new StringBuilder();;
 		private  String fieldDelim;
 		private int ord;
		private long sum;
		private long count;
		private long sumSq;
        private int[] quantityAttrOrdinals;
        private int index;
        private Map<Integer, LongRunningStats> runningStats = new HashMap<Integer, LongRunningStats>();
        private boolean  handleMissingIncremental;
        private int recType;
        private int recCount;
        private static final Logger LOG = Logger.getLogger(RunningAggregator.AggrReducer.class);
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }
        	fieldDelim = config.get("field.delim.out", ",");
        	quantityAttrOrdinals = Utility.intArrayFromString(config.get("quantity.attr.ordinals"));
        	handleMissingIncremental = config.getBoolean("handle.missing.incremental",  false);
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		runningStats.clear();
    		recCount = 0;
    		for (Tuple val : values) {
    			index = 0;
    			recType = val.getInt(index++);
    			
    			//all quant fields
    			for ( int quantOrd : quantityAttrOrdinals) {
    				ord = val.getInt(index++);
    				if (quantOrd != ord) {
    					throw new IllegalStateException("field ordinal does not match");
    				}
    				count = val.getLong(index++);
    				sum  = val.getLong(index++);
    				sumSq  = val.getLong(index++);
    				
    				LongRunningStats stat = runningStats.get(ord);
    				if (null == stat) {
    					runningStats.put(ord, new LongRunningStats(ord, count, sum, sumSq));
    				} else {
    					//LOG.debug("aggregating key: " + key.toString() + " count : " + count + " sum: " + sum + " sumSq: " + sumSq);
    					stat.accumulate(count, sum, sumSq);
    				}
    			}
    			++recCount;
    		}   	
    		
    		//if only aggregate record, increment count by 1
    		if (handleMissingIncremental && recCount == 1 && recType == 0) {
    			for ( int quantOrd : quantityAttrOrdinals) {
    				LongRunningStats stat = runningStats.get(quantOrd);
    				stat.incrementCount();
    			}
    		}
    		
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString()).append(fieldDelim);
    		
    		//all quant field
			for ( int quantOrd : quantityAttrOrdinals) {
				LongRunningStats stat = runningStats.get(quantOrd);
				stat.process();
				stBld.append(stat.getField()).append(fieldDelim).append(stat.getCount()).append(fieldDelim).
				  	append(stat.getSum()).append(fieldDelim).append(stat.getSumSq()).append(fieldDelim).
				  	append(stat.getAvg()).append(fieldDelim).append(stat.getStdDev());
			}
    		
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
		
 	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RunningAggregator(), args);
		System.exit(exitCode);
	}

 
}
