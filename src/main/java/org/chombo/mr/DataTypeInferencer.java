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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.chombo.util.BaseAttribute;
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
    private static final int NUM_TYPES = 9;
    private static final int EPOCH_TIME_TYPE = 8;
    private static final int AGE_TYPE = 7;
    private static final int PHONE_NUM_TYPE = 6;
    private static final int SSN_TYPE = 5;
    private static final int DATE_TYPE = 4;
    private static final int INT_TYPE = 3;
    private static final int FLOAT_TYPE = 2;
    private static final int STRING_TYPE = 1;
    private static final int ANY_TYPE = 0;

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for discovering data types for fields";
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
        private long timeWindowBegin = -1;
        private SimpleDateFormat[] dateFormats;
        private Pattern ssnPattern;
        private Pattern phoneNumPattern;
        private int maxAge = -1;
        
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
        	int epochTimeWindowYears = config.getInt("dti.epoch.time.window.years", -1);
        	if (epochTimeWindowYears > 0) {
        		long now = System.currentTimeMillis();
        		timeWindowBegin = now - epochTimeWindowYears * BasicUtils.MILISEC_PER_DAY * 365;
        	}
        	
        	//date
        	String[] dateFormatStrList = Utility.stringArrayFromString(config, "dti.date.formats", Utility.configDelim);
        	if (null != dateFormatStrList) {
        		int size = dateFormatStrList.length;
        		dateFormats = new SimpleDateFormat[size];
        		for(int i = 0; i < size; ++i) {
        			dateFormats[i] = new SimpleDateFormat(dateFormatStrList[i]);
        		}
        	}
        	
        	//ssn
        	if (config.getBoolean("dti.verify.ssn", true)) {
        		ssnPattern = Pattern.compile(BaseAttribute.PATTERN_STR_SSN);
        	}
        	
        	//phone number
        	if (config.getBoolean("dti.verify.phone.num", true)) {
        		phoneNumPattern = Pattern.compile(BaseAttribute.PATTERN_STR_PHONE_NUM);
        	}

        	//age
        	if (config.getBoolean("dti.verify.age", true)) {
        		maxAge = config.getInt("dti.max.age", 100);
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
        	outVal.initialize();
        	
        	//epoch time
        	boolean isEpoch = timeWindowBegin > 0 && BasicUtils.isLong(value) && Long.parseLong(value) > timeWindowBegin;
        	outVal.add(EPOCH_TIME_TYPE, isEpoch ? 1 : 0);
        	
        	//integer
        	boolean isInt = BasicUtils.isInt(value);
        	outVal.add(INT_TYPE,isInt ? 1 : 0);
        	isNumeric = isInt;
        	
        	//age
        	boolean isAge = isInt && maxAge > 0 && Integer.parseInt(value) <= maxAge;
       		outVal.add(AGE_TYPE, isAge ? 1 : 0);	
       	        	
        	//floating point
        	boolean isFloat = BasicUtils.isFloat(value);
        	outVal.add(FLOAT_TYPE, isFloat ? 1 : 0);
        	isNumeric = isFloat;
        	
        	//date type
        	boolean isDate = false;
        	if (!isNumeric){
 		    	for (SimpleDateFormat dateFormat : dateFormats) {
		    		//date if at least 1 format is able to parse
		    		isDate = BasicUtils.isDate(value, dateFormat);
		    		if (isDate)
		    			break;
		    	}
        	}
        	outVal.add(DATE_TYPE, isDate? 1 : 0);
        	
        	//SSN
        	boolean isSsn = false;
        	if (!isNumeric){
	        	if (null != ssnPattern) {
	        		Matcher matcher = ssnPattern.matcher(value);
	        		isSsn = matcher.matches();
	        	}
        	}
        	outVal.add(SSN_TYPE, isSsn? 1 : 0);
        	
        	//phone number
        	boolean isPhoneNum = false;
        	if (null != phoneNumPattern) {
        		Matcher matcher = phoneNumPattern.matcher(value);
        		isPhoneNum = matcher.matches();
        	}
        	outVal.add(PHONE_NUM_TYPE, isPhoneNum? 1 : 0);

        	//string type
        	outVal.add(STRING_TYPE, isNumeric ? 0 : 1);
        	
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
        	for (int t = NUM_TYPES-1; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	for (int t = NUM_TYPES-1; t >= 0; --t) {
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
    		for (int t = NUM_TYPES-1; t >= 0; --t) {
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
		private int ambiguityThresholdPercent;
		private Map<Integer, Integer> typeCounts = new HashMap<Integer, Integer>();
		private Map<Integer, String> typeNames = new HashMap<Integer, String>();
		private boolean isAmbiguous;
		private double discoveryProb;
		private int anyCount;
		private int intCount;
		private int floatCount;
		private int ambiguityThreshold;
		private StringBuilder stBld =  new StringBuilder();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
			ambiguityThresholdPercent = config.getInt("dti.ambiguity.threshold.percent", 90);
        	for (int t = NUM_TYPES-1; t >= 0; --t) {
        		typeCounts.put(t, 0);
        	}
        	
        	//type names
        	typeNames.put(STRING_TYPE, BaseAttribute.DATA_TYPE_INT);
        	typeNames.put(FLOAT_TYPE, BaseAttribute.DATA_TYPE_FLOAT);
        	typeNames.put(INT_TYPE, BaseAttribute.DATA_TYPE_INT);
        	typeNames.put(DATE_TYPE, BaseAttribute.DATA_TYPE_DATE);
        	typeNames.put(EPOCH_TIME_TYPE, BaseAttribute.DATA_TYPE_EPOCH_TIME);
        	typeNames.put(SSN_TYPE, BaseAttribute.DATA_TYPE_SSN);
        	typeNames.put(PHONE_NUM_TYPE, BaseAttribute.DATA_TYPE_PHONE_NUM);
        	typeNames.put(AGE_TYPE, BaseAttribute.DATA_TYPE_AGE);
		}
	   	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 	*/
		protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
			isAmbiguous = false;
        	for (int t = NUM_TYPES-1; t >= 0; --t) {
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
    		anyCount = typeCounts.get(ANY_TYPE);
    		intCount = typeCounts.get(INT_TYPE);
    		floatCount = typeCounts.get(FLOAT_TYPE);
    		ambiguityThreshold = (anyCount * ambiguityThresholdPercent) / 100;
    		
    		//numeric
    		if (intCount > 0 && floatCount > 0) {
    			if (intCount == anyCount) {
    				//epoch time
    				int discType = discoverType(EPOCH_TIME_TYPE);
    				if (discType > 0) {
    					type = EPOCH_TIME_TYPE;
    				} 
    				
    				//age
    				if (type == STRING_TYPE) {
    					discType = discoverType(AGE_TYPE);
        				if (discType > 0) {
        					type = AGE_TYPE;
        				} 
    				}
    				
    				//int
    				if (type == STRING_TYPE) {
    					type = INT_TYPE;
    				}
    			} else if (floatCount == anyCount){
    				//only some can be interpreted as int
    				type = FLOAT_TYPE;
    			}
    		} 
    		
    		//string
    		if (type == STRING_TYPE) {
    			//can all be interpreted as date
				int discType = discoverType(DATE_TYPE);
				if (discType > 0) {
					type = DATE_TYPE;
				}

    			//ssn
    			if (type == STRING_TYPE) {
    				discType = discoverType(SSN_TYPE);
    				if (discType > 0) {
    					type = SSN_TYPE;
    				}
    			}
    			
    			//phone number
    			if (type == STRING_TYPE) {
    				discType = discoverType(PHONE_NUM_TYPE);
    				if (discType > 0) {
    					type = PHONE_NUM_TYPE;
    				}
    			}
    		}
    		
    		stBld.delete(0, stBld.length());
    		stBld.append(key.get()).append(fieldDelim).append(typeNames.get(type));
    		if (isAmbiguous) {
    			stBld.append(" (ambiguous with correctness probability ").
    				append(BasicUtils.formatDouble(discoveryProb)).append(" ");
    		}
    		
    		outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
		}
		
		/**
		 * @param type
		 * @return
		 */
		private int discoverType(int type) {
			int dicoveredType = -1;
			int typeCount = typeCounts.get(type);
			if (typeCount == anyCount) {
				dicoveredType = type;
			} else if (typeCount > ambiguityThreshold) {
				dicoveredType = type;
				isAmbiguous = true;
				discoveryProb = (typeCount * 100.0) / anyCount;
			} 
			return dicoveredType;
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
