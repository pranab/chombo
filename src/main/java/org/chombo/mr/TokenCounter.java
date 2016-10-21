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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts tokens from unstructured text. Can count token along with it's position in the text record.
 * Can create patterns out of text lines. High frequency tokens are literals. Rest are wild cards.
 * @author pranab
 *
 */
public class TokenCounter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Token counter MR";
        job.setJobName(jobName);
        
        job.setJarByClass(TokenCounter.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(TokenCounter.CounterMapper.class);
        job.setReducerClass(TokenCounter.CounterReducer.class);
        job.setCombinerClass(TokenCounter.CounterCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("toc.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class CounterMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {
		private Tuple outKey = new Tuple();
		private IntWritable outVal = new IntWritable(1);
        private String tokenDelimRegex;
        private String[] items;
        private boolean positionalToken;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	tokenDelimRegex = conf.get("toc.token.delim.regex", "\\s+");
        	positionalToken = conf.getBoolean("toc.positional.token", false);
        }        
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(tokenDelimRegex, -1);
            int i = 0;
            for (String item : items) {
                outKey.initialize();
                outKey.append(item);
            	if (positionalToken) {
            		outKey.append(i);
            		++i;
            	}
            	context.write(outKey, outVal);       	
            }
        }       
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class CounterCombiner extends Reducer<Tuple, IntWritable, Tuple, IntWritable> {
		private int count;
		private IntWritable outVal = new IntWritable();
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context)
        		throws IOException, InterruptedException {
        	count = 0;
        	for (IntWritable value : values) {
        		count += value.get();
        	}
        	outVal.set(count);
        	context.write(key, outVal);       	
        }		
	}	
	
	/**
	 * @author pranab
	 *
	 */
	public static class CounterReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
		private String fieldDelim;
		private Text outVal  = new Text();
		private int count;
		private int countMinThreshold;
		private static final Logger LOG = Logger.getLogger(CounterReducer.class);

	   	/* (non-Javadoc)
	   	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	   	 */
	   	protected void setup(Context context) 
	   			throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
        	fieldDelim = conf.get("field.delim.out", ",");
        	countMinThreshold = conf.getInt("toc.count.min.threshold", -1);
	   	}

	   	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context)
        		throws IOException, InterruptedException {
        	count = 0;
        	for (IntWritable value : values) {
        		count += value.get();
        	}
        	
        	if (countMinThreshold > 0 && count > countMinThreshold || countMinThreshold < 0 ) {
        		key.setDelim(fieldDelim);
        		outVal.set(key.toString() + fieldDelim + count );
        		context.write(NullWritable.get(),outVal);
        	}
        }	
	   	
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TokenCounter(), args);
        System.exit(exitCode);
	}
	
}
