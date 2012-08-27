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
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class Projection extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Projection  and grouping  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Projection.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        String operation = job.getConfiguration().get("projection.operation",  "project");
        
        job.setMapperClass(Projection.ProjectionMapper.class);
        if (operation.startsWith("grouping")) {
            job.setReducerClass(Projection.ProjectionReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        } else {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class ProjectionMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outVal = new Text();
		private int  keyField;
		private int  projectionField;
        private String fieldDelimRegex;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	keyField = context.getConfiguration().getInt("key.field", 0);
        	projectionField = context.getConfiguration().getInt("projection.field", 0);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            outKey.set(items[keyField]);
            outVal.set(items[projectionField]);
			context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class ProjectionReducer extends Reducer<Text, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld;
		private String fieldDelim;

		protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", "[]");
       }
		
    	protected void reduce(Text key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		stBld = new StringBuilder();
    		stBld.append(key);
        	for (Text value : values){
    	   		stBld.append(fieldDelim).append(value);
        	}    		
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    	
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Projection(), args);
        System.exit(exitCode);
	}

}
