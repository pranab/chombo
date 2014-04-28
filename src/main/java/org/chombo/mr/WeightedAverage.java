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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Does weighted average of set of numerical attributes and then sorts by the average value
 * in ascending or descending order
 * @author pranab
 *
 */
public class WeightedAverage extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Weighted average calculating MR";
        job.setJobName(jobName);
        
        job.setJarByClass(WeightedAverage.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(WeightedAverage.AverageMapper.class);
        job.setReducerClass(WeightedAverage.AverageReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
  
        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class AverageMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
		private IntWritable outKey = new IntWritable();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private boolean sortOrderAscending;
        private String[] items;
        private List<Pair<Integer, Integer>> filedWeights;
        private int weightedValue;
        private int sum;
        private int totalWt = 0;
        private static final int ID_FLD_ORDINAL = 0;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	sortOrderAscending = config.getBoolean("sort.order.ascending", true);
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String fieldWeightsStr = config.get("field.weights");
        	filedWeights = Utility.getIntPairList(fieldWeightsStr, ",", ":");
            for (Pair<Integer, Integer> pair : filedWeights) {
            	totalWt  += pair.getRight();
            }
       }
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            sum = 0;
            for (Pair<Integer, Integer> pair : filedWeights) {
            	sum += Integer.parseInt(items[pair.getLeft()]) *   pair.getRight();
            }
            weightedValue = sum / totalWt;
            
            if (sortOrderAscending) {
            	outKey.set(weightedValue);
            } else {
            	outKey.set(Integer.MAX_VALUE - weightedValue);
            }
            outVal.initialize();
            outVal.add(items[ID_FLD_ORDINAL], weightedValue);
			context.write(outKey, outVal);
        }
        
	}	
	
    /**
     * @author pranab
     *
     */
    public static class AverageReducer extends Reducer<IntWritable, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
    	
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(IntWritable key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	for (Tuple value : values){
        		outVal.set(value.toString());
 				context.write(NullWritable.get(), outVal);
        	}    		
    	}
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WeightedAverage(), args);
        System.exit(exitCode);
	}
  
    
}