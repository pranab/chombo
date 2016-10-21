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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Utility;

/**
 * Outputs key and attribute pair
 * @author pranab
 *
 */
public class Combinator extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Combinator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Combinator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        
        job.setMapperClass(Combinator.CombinatorMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class CombinatorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private int  keyField;
		private int[]  keyFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private boolean outputKeyAtBeg;
		private StringBuilder stBld = new  StringBuilder();
        
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	keyField = context.getConfiguration().getInt("com.key.field", 0);
        	keyFields = new int[1];
        	keyFields[0] = keyField;
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = context.getConfiguration().get("field.delim", ",");
        	outputKeyAtBeg = context.getConfiguration().getBoolean("com.output.key.at.begin",true);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            String keyVal = items[keyField];
            String[] otherFields = Utility.filterOutFields(items , keyFields);
            
            for (int i =0; i < otherFields.length; ++i) {
            	for (int j =i+1; j < otherFields.length; ++j) {
    				stBld.delete(0, stBld.length());
    				if (otherFields[i].compareTo(otherFields[j]) < 0) {
            			if (outputKeyAtBeg) {
            				stBld.append(keyVal).append(fieldDelimOut).append(otherFields[i]).append(fieldDelimOut).append(otherFields[j]);
            			} else {
            				stBld.append(otherFields[i]).append(fieldDelimOut).append(otherFields[j]).append(fieldDelimOut).append(keyVal);
            			}
            		} else {
            			if (outputKeyAtBeg) {
            				stBld.append(keyVal).append(fieldDelimOut).append(otherFields[j]).append(fieldDelimOut).append(otherFields[i]);
            			} else {
            				stBld.append(otherFields[j]).append(fieldDelimOut).append(otherFields[i]).append(fieldDelimOut).append(keyVal);
            			}
            		}
    	            outVal.set(stBld.toString());
    				context.write(NullWritable.get(), outVal);
            	}
            }
        }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Combinator(), args);
        System.exit(exitCode);
	}
	
}
