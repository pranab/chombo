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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
import org.chombo.redis.RedisCache;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Simple query with projection. Can do group by and order by.  If grouped by can do count or 
 * unique count. sum and avearge
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
        
        if (operation.startsWith("grouping")) {
        	//group by
            job.setMapperClass(Projection.ProjectionMapper.class);
            job.setReducerClass(Projection.ProjectionReducer.class);

            job.setMapOutputKeyClass(Tuple.class);
            job.setMapOutputValueClass(Text.class);

            int numReducer = job.getConfiguration().getInt("pro.num.reducer", -1);
            numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
            job.setNumReduceTasks(numReducer);
            
            //order by
        	boolean doOrderBy = job.getConfiguration().getInt("orderBy.field", -1) >= 0;
        	if (doOrderBy) {
                job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
                job.setPartitionerClass(SecondarySort.TupleTextPartitioner.class);
        	}

        } else {
        	//simple projection
            job.setMapperClass(Projection.SimpleProjectionMapper.class);
        }
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SimpleProjectionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private int  keyField;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;

        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	keyField = config.getInt("key.field", 0);
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim", ",");
        	projectionFields = Utility.intArrayFromString(config.get("projection.field"),fieldDelimRegex );
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
	        outVal.set(items[keyField] + fieldDelimOut +  Utility.extractFields(items , projectionFields, 
	        		fieldDelimOut, false));
	        context.write(NullWritable.get(), outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class ProjectionMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
		private int  keyField;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private int orderByField;
        private boolean groupBy;
        private boolean isOrderByFieldNumeric;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	String operation = config.get("projection.operation",  "project");
        	groupBy = operation.startsWith("grouping");
        	
        	keyField = config.getInt("key.field", 0);
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim.out", ",");
        	projectionFields = Utility.intArrayFromString(config.get("projection.field"),fieldDelimRegex );
        	orderByField = config.getInt("orderBy.field", -1);
        	isOrderByFieldNumeric = config.getBoolean("orderBy.filed.numeric", false);
       }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
        	outKey.initialize();
            if (orderByField >= 0) {
            	if (isOrderByFieldNumeric) {
               		outKey.add(items[keyField],Double.parseDouble( items[orderByField]));
            	} else {
            		outKey.add(items[keyField], items[orderByField]);
            	}
            } else {
            	outKey.add(items[keyField]);
            }
        	outVal.set( Utility.extractFields(items , projectionFields, fieldDelimOut, false));
        	context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class ProjectionReducer extends Reducer<Tuple, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private RedisCache redisCache;
		private String aggregateValueKeyPrefix;
		private String[] aggrFunctions;
		private int[] aggrFunctionValues;
		private int[] aggrFunctionValuesMax;
		private List<String> strValues = new ArrayList<String>();
		private List<Integer> intValues = new ArrayList<Integer>();
		private Set<String> strValuesSet = new HashSet<String>();
		private int sum;
		private int sqSum;
		private  boolean sortOrderAscending;
		private List<String> sortedValues = new ArrayList<String>();
		private int limitTo;
		private boolean formatCompact;
		private int averageFunctionIndex;
		private double  stdDev;
		private boolean useRank;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", "[]");
        	if (!StringUtils.isBlank(config.get("agrregate.fumctions"))) {
        		aggrFunctions = config.get("agrregate.fumctions").split(fieldDelim);
        		aggrFunctionValues = new int[aggrFunctions.length];
        		aggrFunctionValuesMax = new int[aggrFunctions.length];
        		for (int i = 0; i < aggrFunctionValuesMax.length;  ++i) {
        			aggrFunctionValuesMax[i] = Integer.MIN_VALUE;
        		}
				aggregateValueKeyPrefix = config.get("aggregate.value.key.prefix");
	        	redisCache = RedisCache.createRedisCache(config, "ch");
        	}
        	sortOrderAscending = config.getBoolean("sort.order.ascending", true);
        	limitTo = config.getInt("limit.to", -1);
        	formatCompact = config.getBoolean("format.compact", true);
        	useRank = config.getBoolean("use.rank", false);
       }

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (null != aggrFunctions) {
				for (int i = 0; i < aggrFunctions.length; ++i) {
					redisCache.put(aggregateValueKeyPrefix + "." +aggrFunctions[i] ,  "" + aggrFunctionValuesMax[i], true);
				}
			}
		}
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		if (null != aggrFunctions) {
    			//aggregate functions
        		stBld.delete(0, stBld.length());
        		stBld.append(key.getString(0));

        		strValues.clear();
    			intValues.clear();
    			strValuesSet.clear();
    			sum = 0;
    			sqSum = 0;
    			averageFunctionIndex = -1;
	        	for (Text value : values){
	        		strValues.add(value.toString());
	        	}    			
	        	
	        	//all aggregate functions
	        	for (int i = 0; i <  aggrFunctions.length; ++i) {
	        		if (aggrFunctions[i].equals("count")) {
	        			//count
	        			aggrFunctionValues[i] = strValues.size(); 
	        		} else if (aggrFunctions[i].equals("uniqueCount")) {
	        			//unique count
	        			for (String stVal : strValues) {
	        				strValuesSet.add(stVal);
	        			}
	        			aggrFunctionValues[i] = strValuesSet.size(); 
	        		} else if (aggrFunctions[i].equals("sum")) {
	        			//sum
	        			doSum();
	        			aggrFunctionValues[i] = sum; 
	        		} else if (aggrFunctions[i].equals("average")) {
	        			//average
	        			if (sum == 0) {
		        			doSum();
	        			}
	        			aggrFunctionValues[i] = sum / intValues.size() ; 
	        		} else if (aggrFunctions[i].equals("stdDev")) {
	        			//standard deviation
	        			if (averageFunctionIndex < 0) {
	        				throw new IllegalStateException("average aggregate function must be included for std deviation");
	        			}
		        		doSqSum();
	        			stdDev = (double)sqSum / intValues.size() -  (double)aggrFunctionValues[averageFunctionIndex] * 
	        					aggrFunctionValues[averageFunctionIndex];
	        			stdDev = Math.sqrt(stdDev);
	        			aggrFunctionValues[i] = (int)stdDev ; 
	        		}
	        	}
	        	for (int i = 0; i < aggrFunctionValues.length; ++i) {
	        		if (aggrFunctionValues[i] > aggrFunctionValuesMax[i]) {
	        			aggrFunctionValuesMax[i] =  aggrFunctionValues[i]; 
	        		}
	    	   		stBld.append(fieldDelim).append(aggrFunctionValues[i]);
	        	}
	        	outVal.set(stBld.toString());
				context.write(NullWritable.get(), outVal);
    		}  else {
       			//actual values
    			 if (formatCompact) {
    				emitCompactFormat( key,  values, context);
    			} else {
    				emitLongFormat( key,  values, context);
    			}
    		}
    	}

    	/**
    	 * emits actual values in compact format
    	 * @param key
    	 * @param values
    	 * @param context
    	 * @throws InterruptedException 
    	 * @throws IOException 
    	 */
    	private void emitCompactFormat(Tuple key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
			//actual values
    		stBld.delete(0, stBld.length());
    		stBld.append(key.getString(0));
			if (sortOrderAscending) {
				int i = 0;
	        	for (Text value : values){
	        		if (i == limitTo) {
	        			break;
	        		}
	    	   		stBld.append(fieldDelim).append(value);
	    	   		++i;
	        	}    		
			} else {
				sortedValues.clear();
	        	for (Text value : values){
	        		sortedValues.add(value.toString());
	        	}    	
	        	
	        	//reverse order
				int i = 0;
	        	for (int j = sortedValues.size() -1; j >= 0; --j) {
	        		if (i == limitTo) {
	        			break;
	        		}
	    	   		stBld.append(fieldDelim).append(sortedValues.get(j));
	    	   		++i;
	        	}
			}
	       	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    	
    	/**
    	 * emits actual values in long format
    	 * @param key
    	 * @param values
    	 * @param context
    	 * @throws InterruptedException 
    	 * @throws IOException 
    	 */
    	private void emitLongFormat(Tuple key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
			//actual values
 			if (sortOrderAscending) {
				int i = 0;
	        	for (Text value : values){
	        		if (i == limitTo) {
	        			break;
	        		}
	        		stBld.delete(0, stBld.length());
	        		stBld.append(key.getString(0));
	        		if (useRank) {
	        			//rank
	        			stBld.append(fieldDelim).append(i+1);
	        		} else {
	        			//actual value
	        			stBld.append(fieldDelim).append(value);
	        		}
	    	       	outVal.set(stBld.toString());
	    			context.write(NullWritable.get(), outVal);
	    	   		++i;
	        	}    		
			} else {
				sortedValues.clear();
	        	for (Text value : values){
	        		sortedValues.add(value.toString());
	        	}    	
	        	
	        	//reverse order
				int i = 0;
	        	for (int j = sortedValues.size() -1; j >= 0; --j) {
	        		if (i == limitTo) {
	        			break;
	        		}
	        		stBld.delete(0, stBld.length());
	        		stBld.append(key.getString(0));
	        		if (useRank) {
	        			//rank
	        			stBld.append(fieldDelim).append(i+1);
	        		} else {
	        			//actual value
	        			stBld.append(fieldDelim).append(sortedValues.get(j));
	        		}
	    	       	outVal.set(stBld.toString());
	    			context.write(NullWritable.get(), outVal);
	    	   		++i;
	        	}
			}
    	}

    	/**
    	 * 
    	 */
    	private void doSum() {
			if (intValues.isEmpty()) {
				initializeIntValues();
			}
			for (int intVal : intValues) {
				sum += intVal;
			}
    	}

    	/**
    	 * 
    	 */
    	private void doSqSum() {
			if (intValues.isEmpty()) {
				initializeIntValues();
			}
			for (int intVal : intValues) {
				sqSum += intVal * intVal;
			}
    	}

    	/**
    	 * 
    	 */
    	private void initializeIntValues() {
			for (String stVal : strValues) {
				intValues.add(Integer.parseInt(stVal));
			}
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
