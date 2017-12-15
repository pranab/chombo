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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Inferences data type of fields
 * @author pranab
 *
 */
public class DataTypeInferencer extends Configured implements Tool {
    private static final int NUM_TYPES = 6;
    private static final int EPOCH_TIME_TYPE = 5;
    private static final int DATE_TYPE = 4;
    private static final int INT_TYPE = 3;
    private static final int FLOAT_TYPE = 2;
    private static final int STRING_TYPE = 1;
    private static final int ANY_TYPE = 0;

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
        private long timeWindowBegin;
        private SimpleDateFormat[] dateFormats;
        
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
        	
        	//epoch time
        	int epochTimeWindowYears = config.getInt("dti.epoch.time.window.years", 10);
        	long now = System.currentTimeMillis();
        	timeWindowBegin = now - epochTimeWindowYears * BasicUtils.MILISEC_PER_DAY * 365;
        	
        	//date
        	String[] dateFormatStrList = Utility.stringArrayFromString(config, "dti.date.formats", Utility.configDelim);
        	if (null != dateFormatStrList) {
        		int size = dateFormatStrList.length;
        		dateFormats = new SimpleDateFormat[size];
        		for(int i = 0; i < size; ++i) {
        			dateFormats[i] = new SimpleDateFormat(dateFormatStrList[i]);
        		}
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
        	
        	//epoch time
        	boolean isEpoch = false;
        	if (BasicUtils.isLong(value)) {
        		long lValue = Long.parseLong(value);
        		isEpoch = lValue > timeWindowBegin;
        	}
        	if (isEpoch) {
        		outVal.add(EPOCH_TIME_TYPE, 1);
        	} else {
        		outVal.add(EPOCH_TIME_TYPE, 0);
        	}
        	
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
        	
        	//date type
        	boolean isDate = false;
        	for (SimpleDateFormat dateFormat : dateFormats) {
        		//date if at least 1 format is able to parse
        		isDate = BasicUtils.isDate(value, dateFormat);
        		if (isDate)
        			break;
        	}
        	if (isDate) {
            	outVal.add(DATE_TYPE, 1);
            } else {
            	outVal.add(DATE_TYPE, 0);
            }
        	
        	
        	//string type
        	if (isNumeric) {
        		outVal.add(STRING_TYPE, 0);
        	} else {
        		outVal.add(STRING_TYPE, 1);
        	}
        	
        	//any type
        	outVal.add(ANY_TYPE, 1);
        	
        	context.write(outKey, outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class InferenceCombiner extends Reducer<IntWritable, Tuple,IntWritable, Tuple> {
		private Tuple outVal = new Tuple();
		private Map<Integer, Integer> typeCounts = new HashMap<Integer, Integer>();
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	for (int t = EPOCH_TIME_TYPE; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	for (int t = EPOCH_TIME_TYPE; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
    		for (Tuple val : values) {
            	int offset = 0;
    			for (int i = 0; i < NUM_TYPES; ++i) {
    				int type = val.getInt(offset++);
    				int count =  val.getInt(offset++);
    				typeCounts.put(type, typeCounts.get(type) + count);
    			}
    		}
    		
    		outVal.initialize();
    		for (int t = EPOCH_TIME_TYPE; t >= 0; --t) {
    			outVal.add(t, typeCounts.get(t));
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
		private Map<Integer, Integer> typeCounts = new HashMap<Integer, Integer>();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	for (int t = EPOCH_TIME_TYPE; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
		}
	   	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 	*/
		protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
        	for (int t = EPOCH_TIME_TYPE; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
    		for (Tuple val : values) {
            	int offset = 0;
    			for (int i = 0; i < NUM_TYPES; ++i) {
    				int type = val.getInt(offset++);
    				int count =  val.getInt(offset++);
    				typeCounts.put(type, typeCounts.get(type) + count);
    			}
    		}
    		
    		//find type
    		int type = STRING_TYPE;
    		int anyCount = typeCounts.get(ANY_TYPE);
    		int intCount = typeCounts.get(INT_TYPE);
    		int floatCount = typeCounts.get(FLOAT_TYPE);
    		int epochTimeCount = typeCounts.get(EPOCH_TIME_TYPE);
    		if (intCount > 0 && floatCount > 0) {
    			if (intCount == anyCount) {
    				//all can interpreted as int
    				type =  epochTimeCount == anyCount ? EPOCH_TIME_TYPE : INT_TYPE;
    			} else if (floatCount == anyCount){
    				//only some can be interpreted as int
    				type = FLOAT_TYPE;
    			}
    		} 
    		if (type == STRING_TYPE) {
    			//can all be interpreted as date
    			int dateCount = typeCounts.get(DATE_TYPE);
    			type =  dateCount == anyCount ? DATE_TYPE : STRING_TYPE;
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
