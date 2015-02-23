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
        
        if (job.getConfiguration().getInt("group.by.field",  -1) >=  0) {
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
        private int weightedValue;
        private int sum;
        private int totalWt = 0;
        private int[] invertedFields;
        private int fieldValue;
        private int  scale;
        // private static final int ID_FLD_ORDINAL = 0;
		private RedisCache redisCache;
        private Map<Integer, Integer> fieldMaxValues =  new HashMap<Integer, Integer>();
        private boolean singleTennant;
        private int fieldOrd;
        private int[] suppressingFields;
        private int secondaryKey;
        private int[] keyFields;
        private Integer maxValue;
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

        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	sortOrderAscending = config.getBoolean("sort.order.ascending", true);
        	scale = config.getInt("field.scale", 100);
        	keyFields = Utility.intArrayFromString(config.get("key.fields"));
        	groupByField = config.getInt("group.by.field", -1);
	   		LOG.debug("keyFields:" + keyFields + " groupByField:" + groupByField);
	   		        	
        	//field weights
        	String fieldWeightsStr = config.get("field.weights");
        	filedWeights = Utility.getIntPairList(fieldWeightsStr, ",", ":");
            for (Pair<Integer, Integer> pair : filedWeights) {
            	totalWt  += pair.getRight();
   	   			LOG.debug("field:" + pair.getLeft() + " weight:" + pair.getRight());
            }
            
            //inverted fields
        	String invertedFieldsStr = config.get("inverted.fields");
        	if (!Utility.isBlank(invertedFieldsStr)) {
            	invertedFields = Utility.intArrayFromString(invertedFieldsStr);
        	}
        	
        	//suppressing fields
        	String suppressingFieldsStr = config.get("suppressing.fields");
        	if (!Utility.isBlank(suppressingFieldsStr)) {
        		suppressingFields = Utility.intArrayFromString(suppressingFieldsStr);
        	}
            
        	//field max values from cache
        	String fieldMaxValuesCacheKey = config.get("field.max.values.cache.key");
        	List<Pair<Integer, String>> filedMaxValueKeys = Utility.getIntStringList(fieldMaxValuesCacheKey, ",", ":");
    		String redisHost = config.get("redis.server.host", "localhost");
    		int redisPort = config.getInt("redis.server.port",  6379);
    		String defaultOrgId = config.get("default.org.id");
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
       }
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            sum = 0;
            for (Pair<Integer, Integer> pair : filedWeights) {
            	fieldOrd = pair.getLeft();
            	fieldValue = Integer.parseInt(items[fieldOrd]);
            	
            	//if suppressing field and value is 0 then skip  the record
            	if (null != suppressingFields && ArrayUtils.contains(suppressingFields, fieldOrd) && fieldValue == 0 ) {
        			context.getCounter("Record stat", "Suppressed").increment(1);
        			return;
            	}
            	
            	//scale field value
            	if (singleTennant) {
            		maxValue = fieldMaxValues.get(fieldOrd);
            		if (null != maxValue) {
            			fieldValue =  (fieldValue * scale) / maxValue;
            		}
            	} else {
            		
            	}
            	
            	//invert
            	if (null != invertedFields && ArrayUtils.contains(invertedFields, fieldOrd)) {
            		fieldValue = scale - fieldValue;
            	}
            	sum += fieldValue *   pair.getRight();
            }
            weightedValue = sum / totalWt;
            
            //key
            outKey.initialize();
            secondaryKey  = sortOrderAscending ? weightedValue  :  scale  - weightedValue;
            if (groupByField >= 0) {
            	outKey.add(items[groupByField], secondaryKey);
            } else {
            	outKey.add(secondaryKey);
            }
            
            //value
            outVal.initialize();
            for (int keyField : keyFields) {
            	outVal.add(items[keyField]);
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
    	
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	for (Tuple value : values){
        		outVal.set(value.toString());
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