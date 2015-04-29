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
import org.chombo.util.SecondarySort;
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
        private int quantityAttr;
        private boolean isAggrFileSplit;
        private int[] idFieldOrdinals;
        private boolean doStdDev;
        private long newValue;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	quantityAttr = config.getInt("quantity.attr",-1);
        	String aggrFilePrefix = config.get("aggregate.file.prefix", "");
        	if (!aggrFilePrefix.isEmpty()) {
        		isAggrFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(aggrFilePrefix);
        	} else {
            	String incrFilePrefix = config.get("incremental.file.prefix", "");
            	if (!incrFilePrefix.isEmpty()) {
            		isAggrFileSplit = !((FileSplit)context.getInputSplit()).getPath().getName().startsWith(incrFilePrefix);
            	} else {
            		throw new IOException("Aggregate or incremental file prefix needs to be specified");
            	}
        	}
        	
        	if (null != config.get("id.field.ordinals")) {
        		idFieldOrdinals = Utility.intArrayFromString(config.get("id.field.ordinals"));
        	}
        	
        	doStdDev = config.getBoolean("std.dev", false);
       }
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	outKey.initialize();
        	outVal.initialize();
        	int initValue = 0;
        	if (null != idFieldOrdinals) {
      			for (int ord : idFieldOrdinals ) {
      				outKey.append(items[ord]);
      			}
        	} else {
        		//all fields before quantity are ID fields
	            for (int i = 0; i < quantityAttr;  ++i) {
	            	outKey.append(items[i]);
	            }
        	}
        	if (isAggrFileSplit) {
        		if (items.length >= quantityAttr) {
        			//existing aggregation - count, sum
                    outVal.add(Long.parseLong(items[quantityAttr]) ,   Long.parseLong(items[quantityAttr + 1]) );
                    if (doStdDev) {
                    	//sum square
                    	outVal.add(Long.parseLong(items[quantityAttr + 2]));
                    }
        		} else {
        			//first aggregation
        			outVal.add(initValue, initValue);
                    if (doStdDev) {
                    	outVal.add(initValue);
                    }
        		}
        	} else {
        		//incremental
        		newValue = Long.parseLong( items[quantityAttr]);
                outVal.add((long)1, newValue);
                if (doStdDev) {
                	outVal.add(newValue * newValue);
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
		private long sum;
		private long count;
		private long sumSq;
		private long avg;
        private boolean doStdDev;
        private double stdDev;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	doStdDev = config.getBoolean("std.dev", false);
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		sum = 0;
    		sumSq = 0;
    		count = 0;
    		for (Tuple val : values) {
				count += val.getLong(0);
    			sum  += val.getLong(1);
    			if (doStdDev) {
        			sumSq  += val.getLong(2);
    			}
    		}   	
    		
    		if (count > 0) {
	    		avg =  sum / count;
	    		if (doStdDev) {
	    			if (1 == count) {
	    				stdDev = 0;
	    			} else {
	    				double ave = (double)sum / count;
	    				stdDev = (double)sumSq / count -  ave * ave;
	    				stdDev = Math.sqrt(stdDev);
	    			}
	    		}
    		} else {
    			avg = 0;
    			stdDev = 0;
    		}
    		
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString()).append(fieldDelim);
    		stBld.append(count).append(fieldDelim).append(sum).append(fieldDelim);
    		if (doStdDev) {
    			stBld.append(sumSq);
    		}
    		stBld.append(avg);
    		if (doStdDev) {
    			stBld.append(stdDev);
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
