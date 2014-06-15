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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.SecondarySort;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Joins two sets of records. Join can be done with one or more keys
 * @author pranab
 *
 */
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
	        job.setReducerClass(Joiner.JoinerReducer.class);

	        job.setMapOutputKeyClass(TextInt.class);
	        job.setMapOutputValueClass(Tuple.class);

	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);

	        job.setGroupingComparatorClass(SecondarySort.TextIntIdPairGroupComprator.class);
	        job.setPartitionerClass(SecondarySort.TextIntIdPairPartitioner.class);

	        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
	        
	        int status =  job.waitForCompletion(true) ? 0 : 1;
	        return status;
		}
		
		public static class JoinerMapper extends Mapper<LongWritable, Text, TextInt, Tuple> {
			private TextInt outKey = new TextInt();
			private Tuple outVal = new Tuple();
			private int[]  keyFieldFirst;
			private int[]  keyFieldSecond;
	        private String fieldDelimRegex;
	        private String fieldDelimOut;
	        private boolean isFirstTypeSplit;
	        private boolean sortKeyFields;
	        
	        protected void setup(Context context) throws IOException, InterruptedException {
	        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
	        	fieldDelimOut = context.getConfiguration().get("field.delim", ",");
	        	String firstTypePrefix = context.getConfiguration().get("first.type.prefix", "first");
	        	isFirstTypeSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(firstTypePrefix);
	        	keyFieldFirst = Utility.intArrayFromString(context.getConfiguration().get("key.field.first"), fieldDelimRegex ); 
	        	keyFieldSecond = Utility.intArrayFromString(context.getConfiguration().get("key.field.second"), fieldDelimRegex ); 
	        	sortKeyFields = context.getConfiguration().getBoolean("sort.key.fields", false);
	       }

	        @Override
	        protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	            String[] items  =  value.toString().split(fieldDelimRegex);
	            if (isFirstTypeSplit) {
	            	outKey.set(Utility.extractFields(items , keyFieldFirst, fieldDelimOut, sortKeyFields) , 0);
	            	Utility.createTuple(items, keyFieldFirst, outVal);
	            	outVal.prepend("0");
	            } else {
	            	outKey.set(Utility.extractFields(items , keyFieldSecond, fieldDelimOut, sortKeyFields) , 1);
	            	Utility.createTuple(items, keyFieldSecond, outVal);
	            	outVal.prepend("1");
	            }
	            
	    		context.write(outKey, outVal);
	        }
		}

	    /**
	     * @author pranab
	     *
	     */
	    public static class JoinerReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
			private Text outVal = new Text();
	    	private List<Tuple> fistTypeList = new ArrayList<Tuple>();
			private int[]  keyFieldFirst;
			private int[]  keyFieldSecond;
	        private String fieldDelimRegex;
			private String fieldDelimOut;
			private Tuple secondType;
			private StringBuilder stBld = new  StringBuilder();
			private boolean outputKeyAtBeg;
			private boolean outputFirstType;
			private boolean outputSecondType;
	    	
	        protected void setup(Context context) throws IOException, InterruptedException {
	        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
	        	fieldDelimOut = context.getConfiguration().get("field.delim", ",");
	        	keyFieldFirst = Utility.intArrayFromString(context.getConfiguration().get("key.field.first"), fieldDelimRegex ); 
	        	keyFieldSecond = Utility.intArrayFromString(context.getConfiguration().get("key.field.second"), fieldDelimRegex ); 
	        	outputKeyAtBeg = context.getConfiguration().getBoolean("output.key.at.begin",true);
	        	outputFirstType = context.getConfiguration().getBoolean("output.first.type",true);
	        	outputSecondType = context.getConfiguration().getBoolean("output.second.type",true);
	       }

	        protected void reduce(TextInt key, Iterable<Tuple> values, Context context)
	        	throws IOException, InterruptedException {
	        	fistTypeList.clear();
	        	for (Tuple value : values){
	        		if (value.startsWith("0")) {
	        			fistTypeList.add(value);
	        		} else {
	        			secondType = value;
 	        			for (Tuple firstType :  fistTypeList) {
	        				stBld.delete(0, stBld.length());
	        				
	        				if (outputKeyAtBeg) {
	        					stBld.append(key.getFirst()).append(fieldDelimOut);
	        				}
	        				if (outputFirstType) {
	        					stBld.append(firstType.toString(1)).append(fieldDelimOut);
	        				}
	        				if (outputSecondType) {
	        					stBld.append(secondType.toString(1));
	        				}
	        				if (!outputKeyAtBeg) {
	        					if(outputSecondType) {
	        						stBld.append(fieldDelimOut);
	        					}
	        					stBld.append(key.getFirst());
	        				}
	        				
	        				outVal.set(stBld.toString());
	    	 				context.write(NullWritable.get(), outVal);
	        			}
	        		}
	        	}    		
	    	}
	    	
	    }

		/**
		 * @param args
		 */
		public static void main(String[] args) throws Exception {
	        int exitCode = ToolRunner.run(new Joiner(), args);
	        System.exit(exitCode);
		}

}
