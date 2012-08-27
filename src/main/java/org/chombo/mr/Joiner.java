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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.chombo.util.SecondarySort;
import org.chombo.util.TextInt;
import org.chombo.util.Utility;

public class Joiner extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
	        Job job = new Job(getConf());
	        String jobName = "Joiner  MR";
	        job.setJobName(jobName);
	        
	        job.setJarByClass(Joiner.class);

	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        Utility.setConfiguration(job.getConfiguration());
	        
	        job.setMapperClass(Joiner.JoinerMapper.class);
	        job.setReducerClass(Projection.ProjectionReducer.class);

	        job.setMapOutputKeyClass(TextInt.class);
	        job.setMapOutputValueClass(Text.class);

	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);

	        job.setGroupingComparatorClass(SecondarySort.TextIntIdPairGroupComprator.class);
	        job.setPartitionerClass(SecondarySort.TextIntIdPairPartitioner.class);

	        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
	        
	        int status =  job.waitForCompletion(true) ? 0 : 1;
	        return status;
		}
		
		public static class JoinerMapper extends Mapper<LongWritable, Text, TextInt, Text> {
			private TextInt outKey = new TextInt();
			private Text outVal = new Text();
			private int keyFieldFirst;
			private int keyFieldSecond;
	        private String fieldDelimRegex;
	        private boolean isFirstTypeSplit;
	        
	        protected void setup(Context context) throws IOException, InterruptedException {
	        	String firstTypePrefix = context.getConfiguration().get("first.type.prefix", "first");
	        	isFirstTypeSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(firstTypePrefix);
	        	keyFieldFirst = context.getConfiguration().getInt("key.field.first", 0);
	        	keyFieldSecond = context.getConfiguration().getInt("key.field.second", 0);
	        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
	       }

	        @Override
	        protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	            String[] items  =  value.toString().split(fieldDelimRegex);
	            if (isFirstTypeSplit) {
	            	outKey.set(items[keyFieldFirst], 0);
	            	outVal.set("0," + value.toString());
	            } else {
	            	outKey.set(items[keyFieldSecond],1);
	            	outVal.set("1," + value.toString());
	            }
	            
	    		context.write(outKey, outVal);
	        }
		}

	    public static class JoinerReducer extends Reducer<TextInt, Text, NullWritable, Text> {
			private Text outVal = new Text();
	    	private List<String> fistTypeList = new ArrayList<String>();
			private int keyFieldFirst;
			private int keyFieldSecond;
	        private String fieldDelimRegex;
			private String fieldDelimOut;
			private String secondType;
			private StringBuilder stBld = new  StringBuilder();
	    	
	        protected void setup(Context context) throws IOException, InterruptedException {
	        	keyFieldFirst = context.getConfiguration().getInt("key.field.first", 0);
	        	keyFieldSecond = context.getConfiguration().getInt("key.field.second", 0);
	        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
	        	fieldDelimOut = context.getConfiguration().get("field.delim.regex", ",");
	       }

	        protected void reduce(TextInt key, Iterable<Text> values, Context context)
	        	throws IOException, InterruptedException {
	        	fistTypeList.clear();
	        	for (Text value : values){
	        		if (value.toString().startsWith("0")) {
	        			fistTypeList.add(value.toString().substring(2));
	        		} else {
	        			secondType = value.toString().substring(2);
 	        			for (String firstType :  fistTypeList) {
	        				stBld.delete(0, stBld.length());
	        				stBld.append(key.getFirst()).append(fieldDelimOut).append(Utility.removeField(firstType, keyFieldFirst,fieldDelimRegex,  fieldDelimOut)).
	        					append(fieldDelimOut).append(Utility.removeField(secondType, keyFieldSecond, fieldDelimRegex, fieldDelimOut));
	        				outVal.set(stBld.toString());
	    	 				context.write(NullWritable.get(), outVal);
	        			}
	        		}
	        	}    		
	    	}
	    	
	    }


}
