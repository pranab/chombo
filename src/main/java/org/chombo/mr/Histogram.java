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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author pranab
 *
 */
public class Histogram extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Histogram MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Histogram.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(Histogram.HistogramMapper.class);
        job.setReducerClass(Histogram.HistogramReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("his.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class HistogramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text outKey = new Text();
		private IntWritable outVal = new IntWritable(1);
        private String fieldDelim;
        private String fieldDelimRegex;
        private RichAttributeSchema schema;
        private IntWritable one = new IntWritable(1);
        private int bucket;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", "[]");
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
            
        	String filePath = conf.get("histogram.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, RichAttributeSchema.class);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            for (RichAttribute field : schema.getFields()) {
            	if (field.isCategorical()){
            		outKey.set("" + field.getOrdinal() +fieldDelim + items[field.getOrdinal()]);
            	} else if (field.isInteger()) {
            		bucket = Integer.parseInt(items[field.getOrdinal()]) /  field.getBucketWidth();
            		outKey.set("" + field.getOrdinal() +fieldDelim + bucket);
            	} else if (field.isDouble()) {
            		bucket = ((int)Double.parseDouble(items[field.getOrdinal()])) /  field.getBucketWidth();
            		outKey.set("" + field.getOrdinal() +fieldDelim + bucket);
            	}
     			context.write(outKey, outVal);
            }
        }
	}
	
    public static class HistogramReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
    	private Text valueOut = new Text();
    	private int sum;
    	private String fieldDelim ;

        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", "[]");
        }    	
    	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        	throws IOException, InterruptedException {
    		sum = 0;
        	for (IntWritable value : values){
        		sum += value.get();
        	}    		
        	valueOut.set(key.toString() + fieldDelim + sum);
			context.write(NullWritable.get(), valueOut);
    	}
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Histogram(), args);
        System.exit(exitCode);
	}
}
