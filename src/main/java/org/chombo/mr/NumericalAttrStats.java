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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Basic stats for numerical attributes
 * @author pranab
 *
 */
public class NumericalAttrStats  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Basic stats for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericalAttrStats.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(NumericalAttrStats.StatsMapper.class);
        job.setReducerClass(NumericalAttrStats.StatsReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class StatsMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private int conditionedAttr;
        private double val;
        private double sqVal;
        private int count = 1;
        private String[] items;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	attributes = Utility.intArrayFromString(config.get("attr.list"),fieldDelimRegex );
        	conditionedAttr = config.getInt("conditioned.attr",-1);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	for (int attr : attributes) {
            	outKey.initialize();
            	outVal.initialize();
            	outKey.add(attr, "0");
            	val = Double.parseDouble(items[attr]);
            	sqVal = val * val;
            	outVal.add(val, sqVal, count);
            	context.write(outKey, outVal);

            	//conditioned on another attribute
            	if (conditionedAttr >= 0) {
                	outKey.initialize();
                	outVal.initialize();
                	outKey.add(attr, items[conditionedAttr]);
                	outVal.add(val, sqVal, count);
                	context.write(outKey, outVal);
            	}
        	}
        }
         
	}
	
	   /**
     * @author pranab
     *
     */
    public static class StatsReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private double sum;
		private double sumSq;
		private int totalCount;
		private double mean;
		private double variance;
		private double stdDev;

		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
       }
		
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		stBld.delete(0, stBld.length());
    		
    		sum = 0;
    		sumSq = 0;
    		totalCount = 0;
    		for (Tuple val : values) {
    			sum  += val.getDouble(0);
    			sumSq += val.getDouble(1);
    			totalCount += val.getInt(2);
    		}
    		mean = sum / totalCount;
    		variance = sumSq / totalCount - mean * mean;
    		stdDev = Math.sqrt(variance);
 
    		stBld.append(key.getInt(0)).append(fieldDelim).append(key.getString(1)).append(fieldDelim);
    		stBld.append(sum).append(fieldDelim).append(sumSq).append(fieldDelim).append(totalCount).append(fieldDelim) ;
    		stBld.append(mean).append(fieldDelim).append(variance).append(fieldDelim).append(stdDev).append(fieldDelim)  ;
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    	
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericalAttrStats(), args);
        System.exit(exitCode);
	}

}
