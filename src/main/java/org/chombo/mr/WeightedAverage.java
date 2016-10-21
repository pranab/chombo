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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.redis.RedisCache;
import org.chombo.util.Pair;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Does weighted average of set of numerical attributes and then sorts by the average value
 * in ascending or descending order. can optionally scale filed values, fetching max values from 
 * cache
 * @author pranab
 *
 */
public class WeightedAverage extends Configured implements Tool {
 
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Weighted average calculating MR";
        job.setJobName(jobName);
        
        job.setJarByClass(WeightedAverage.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(WeightedAverage.AverageMapper.class);
        job.setReducerClass(WeightedAverage.AverageReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
  
        Utility.setConfiguration(job.getConfiguration());
        
        if (job.getConfiguration().getInt("wea.group.by.field",  -1) >=  0) {
        	//group by
	        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
	        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        }

        int numReducer = job.getConfiguration().getInt("wea.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class AverageMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private boolean sortOrderAscending;
        private int groupByField;
        private String[] items;
        private List<Pair<Integer, Integer>> filedWeights;
        private double weightedValue;
        private double sum;
        private int totalWt = 0;
        private int[] invertedFields;
        private double fieldValue;
        private int  scale;
		private RedisCache redisCache;
        private Map<Integer, Integer> fieldMaxValues =  new HashMap<Integer, Integer>();
        private boolean singleTennant;
        private int fieldOrd;
        private int[] suppressingFields;
        private long secondaryKey;
        private int[] keyFields;
        private Integer maxValue;
        private boolean scalingNeeded;
        private boolean maxValueFromcache;
        private static final Logger LOG = Logger.getLogger(WeightedAverage.AverageMapper.class);
       
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("turned debug on");
            }

        	fieldDelimRegex = Utility.getFieldDelimiter(config, "wea.field.delim.regex", "field.delim.regex", ",");
        	sortOrderAscending = config.getBoolean("wea.sort.order.ascending", true);
        	scale = config.getInt("wea.field.scale", 100);
        	keyFields = Utility.intArrayFromString(config.get("wea.key.fields"));
        	groupByField = config.getInt("wea.group.by.field", -1);
	   		LOG.debug("keyFields:" + keyFields + " groupByField:" + groupByField);
	   		        	
        	//field weights
        	String fieldWeightsStr = config.get("wea.field.weights");
        	filedWeights = Utility.getIntPairList(fieldWeightsStr, ",", ":");
            for (Pair<Integer, Integer> pair : filedWeights) {
            	totalWt  += pair.getRight();
   	   			LOG.debug("field:" + pair.getLeft() + " weight:" + pair.getRight());
            }
            
            //inverted fields
        	String invertedFieldsStr = config.get("wea.inverted.fields");
        	if (!Utility.isBlank(invertedFieldsStr)) {
            	invertedFields = Utility.intArrayFromString(invertedFieldsStr);
        	}
        	
        	//suppressing fields
        	String suppressingFieldsStr = config.get("wea.suppressing.fields");
        	if (!Utility.isBlank(suppressingFieldsStr)) {
        		suppressingFields = Utility.intArrayFromString(suppressingFieldsStr);
        	}
            
        	//scaling 
        	scalingNeeded = config.getBoolean("wea.scaling.needed", false);
        	
        	if (scalingNeeded) {
            	maxValueFromcache = config.getBoolean("wea.max.value.from.cache", false);
            	if (maxValueFromcache) {
            		//field max values from cache
            		String fieldMaxValuesCacheKey = config.get("wea.field.max.values.cache.key");
            		List<Pair<Integer, String>> filedMaxValueKeys = Utility.getIntStringList(fieldMaxValuesCacheKey, ",", ":");
            		String redisHost = config.get("wea.redis.server.host", "localhost");
            		int redisPort = config.getInt("wea.redis.server.port",  6379);
            		String defaultOrgId = config.get("wea.default.org.id");
            		singleTennant = false;
            		if (!StringUtils.isBlank(defaultOrgId)) {
            			//default org
            			singleTennant = true;
            			String cacheName = "si-" + defaultOrgId;
            			redisCache = new   RedisCache( redisHost, redisPort, cacheName);
            			for (Pair<Integer, String> pair : filedMaxValueKeys) {
            				int maxValue = redisCache.getIntMax(pair.getRight());
            				fieldMaxValues.put(pair.getLeft(), maxValue);
            				LOG.debug("field:" + pair.getLeft() + " max value:" + maxValue);
            			}
            		} else {
            			//multi organization
    	   		
            		}
            	} else {
            		//from configuration
            		singleTennant = true;
            		fieldMaxValues = Utility.assertIntegerIntegerMapConfigParam(config, "wea.attribute.max.values", 
            				Utility.configDelim, Utility.configSubFieldDelim, "missing max values for scaling", false);
            	}
        	}
       }
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            sum = 0;
            for (Pair<Integer, Integer> pair : filedWeights) {
            	fieldOrd = pair.getLeft();
            	fieldValue = Double.parseDouble(items[fieldOrd]);
            	
            	//if suppressing field and value is 0 then skip  the record
            	if (null != suppressingFields && ArrayUtils.contains(suppressingFields, fieldOrd) && 
            			items[fieldOrd].equals("0") ) {
        			context.getCounter("Record stat", "Suppressed").increment(1);
        			return;
            	}
            	
            	//scale field value if needed
            	if (scalingNeeded) {
	            	if (singleTennant) {
	            		maxValue = fieldMaxValues.get(fieldOrd);
	            		if (null != maxValue) {
	            			fieldValue =  (fieldValue * scale) / maxValue;
	            		}
	            	} else {
	            		
	            	}
            	}
            	
            	//invert if needed
            	if (null != invertedFields && ArrayUtils.contains(invertedFields, fieldOrd)) {
            		fieldValue = scale - fieldValue;
            	}
            	
            	sum += fieldValue *  pair.getRight();
            }
            weightedValue = sum / totalWt;
            weightedValue = weightedValue < 0 ? 0 : weightedValue;
            
            //key
            outKey.initialize();
            long wtVal = (long)(weightedValue * 1000);
            secondaryKey  = sortOrderAscending ? wtVal  :  Long.MAX_VALUE  -  wtVal;
            if (groupByField >= 0) {
            	//secondary sorting by weight
            	outKey.add(items[groupByField], secondaryKey);
            } else {
            	//primary  sorting by weight
            	outKey.add(secondaryKey);
            }
            
            //value
            outVal.initialize();
            if (null != keyFields) {
            	outVal.addFromArray(items, keyFields);
            }
            outVal.add( weightedValue);
			context.write(outKey, outVal);
        }        
	}	
	
    /**
     * @author pranab
     *
     */
    public static class AverageReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private boolean outputAsFloat;
		private int precision;
		private double weightedValue;
		private int keyFieldsLength;
        private int groupByField;
        private boolean outputGroupByField;
		private StringBuilder stBld = new StringBuilder();


		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	keyFieldsLength = Utility.intArrayFromString(config.get("wea.key.fields")).length;
			outputAsFloat = config.getBoolean("wea.output.as.float", true);
			if (outputAsFloat) {
				precision = config.getInt("wea.output.precision", 3);
			}
        	groupByField = config.getInt("wea.group.by.field", -1);
        	outputGroupByField = config.getBoolean("wea.output.group.by.field", false);
		}
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	for (Tuple value : values){
        		stBld.delete(0, stBld.length());
        		
        		if (outputGroupByField && groupByField >= 0) {
               		stBld.append(key.getString(0)).append(fieldDelim);
        		}
        		stBld.append(value.toString(0, keyFieldsLength));
        		weightedValue = value.getDouble(keyFieldsLength);
        		if (outputAsFloat) {
        			stBld.append(fieldDelim).append(Utility.formatDouble(weightedValue, precision));
        		} else {
        			stBld.append(fieldDelim).append("" + (long)weightedValue);
        		}
        		outVal.set(stBld.toString());
 				context.write(NullWritable.get(), outVal);
        	}    		
    	}
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WeightedAverage(), args);
        System.exit(exitCode);
	}
  
    
}