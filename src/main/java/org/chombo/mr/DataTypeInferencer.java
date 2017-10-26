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
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Inferences data type of of fields
 * @author pranab
 *
 */
public class DataTypeInferencer extends Configured implements Tool {
    private static final int INT_TYPE = 2;
    private static final int FLOAT_TYPE = 1;
    private static final int STRING_TYPE = 0;

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for specific  value counting for various fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(ValueCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(DataTypeInferencer.InferenceMapper.class);
        job.setReducerClass(DataTypeInferencer.InferenceReducer.class);
        job.setCombinerClass(DataTypeInferencer.InferenceCombiner.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("dti.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class InferenceMapper extends Mapper<LongWritable, Text,  IntWritable, Tuple> {
		private IntWritable outKey = new IntWritable(1); 
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String[] items;
        private String fieldDelimRegex;
        private boolean allAttributes;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	if (config.get("dti.attr.list", "*").equals("*")) {
        		allAttributes = true;
        	} else {
        		attributes = Utility.intArrayFromString(config, "dti.attr.list", Utility.configDelim);
        	}
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items = value.toString().split(fieldDelimRegex, -1);
            
            if (allAttributes) {
            	for (int i = 0; i < items.length; ++i) {
            		emitOutput(i, items[i], context);
            	}
            } else {
            	for (int i : attributes) {
            		emitOutput(i, items[i], context);
            	}
            }
        }
        
        /**
         * @param ordinal
         * @param value
         * @throws InterruptedException 
         * @throws IOException 
         */
        private void emitOutput(int ordinal, String value, Context context) throws IOException, InterruptedException {
        	boolean isNumeric = false;
        	outKey.set(ordinal);
        	
        	//integer
        	if (BasicUtils.isInt(value)) {
        		outVal.add(INT_TYPE, 1);
        		isNumeric = true;
        	} else {
           		outVal.add(INT_TYPE, 0);
        	}
        	
        	//floating point
        	if (BasicUtils.isFloat(value)) {
        		outVal.add(FLOAT_TYPE, 1);
        		isNumeric = true;
        	} else {
        		outVal.add(FLOAT_TYPE, 0);
        	}
        	
        	//string type
        	if (isNumeric) {
        		outVal.add(STRING_TYPE, 0);
        	} else {
        		outVal.add(STRING_TYPE, 1);
        	}
        	context.write(outKey, outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class InferenceCombiner extends Reducer<IntWritable, Tuple,IntWritable, Tuple> {
		private Tuple outVal = new Tuple();
		private List<Pair<Integer, Integer>> typeCounts = new ArrayList<Pair<Integer, Integer>>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	for (int t = INT_TYPE; t >= 0; --t) {
        		typeCounts.add(new Pair<Integer, Integer>(t, 0));
        	}
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	for (Pair<Integer, Integer> typeCount : typeCounts) {
        		typeCount.setRight(0);
        	}
    		for (Tuple val : values) {
    			int offset = 0;
    			for (int type = INT_TYPE; type >= 0; --type) {
    				Pair<Integer, Integer> typeCount = typeCounts.get(offset / 2);
    				offset++;
    				int count =  val.getInt(offset++);
    				typeCount.setRight(typeCount.getRight() + count);
    			}
    		}
    		outVal.initialize();
    		for (Pair<Integer, Integer> typeCount : typeCounts) {
    			outVal.add(typeCount.getLeft(), typeCount.getRight());
    		}
        	context.write(key, outVal);
        }	
	}
	
	/**
	* @author pranab
	*
	*/
	public static class InferenceReducer extends Reducer<IntWritable, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected String fieldDelim;
		private List<Pair<Integer, Integer>> typeCounts = new ArrayList<Pair<Integer, Integer>>();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	for (int t = INT_TYPE; t >= 0; --t) {
        		typeCounts.add(new Pair<Integer, Integer>(t, 0));
        	}
		}
	   	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 	*/
		protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
        	for (Pair<Integer, Integer> typeCount : typeCounts) {
        		typeCount.setRight(0);
        	}
    		for (Tuple val : values) {
    			int offset = 0;
    			for (int type = INT_TYPE; type >= 0; --type) {
    				Pair<Integer, Integer> typeCount = typeCounts.get(offset / 2);
    				offset++;
    				int count =  val.getInt(offset++);
    				typeCount.setRight(typeCount.getRight() + count);
    			}
    		}
    		
    		//find type
    		int type = STRING_TYPE;
    		int intCount = typeCounts.get(0).getRight();
    		int floatCount = typeCounts.get(1).getRight();
    		if (intCount > 0 && floatCount > 0) {
    			if (intCount == floatCount) {
    				type = INT_TYPE;
    			} else {
    				type = FLOAT_TYPE;
    			}
    		} 
    		
    		outVal.set("" + key.get() + fieldDelim + type);
			context.write(NullWritable.get(), outVal);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DataTypeInferencer(), args);
		System.exit(exitCode);
	}

}
