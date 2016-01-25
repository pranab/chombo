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
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Does data normalization. Can use minmax or zscore normalization. With zscore
 * normalization, additionally outlier can be removed
 * @author pranab
 *
 */
public class Normalizer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Data normalizer MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Normalizer.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(Normalizer.NormalizerMapper.class);
        job.setReducerClass(Normalizer.NormalizerReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
  
        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("nor.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class NormalizerMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private List<Pair<Integer, Integer>> fieldScales;
        private static final int ID_ORD = 0;
        private static final String STATS_KEY = "stats";
        private Map<Integer, Stats> fieldStats = new HashMap<Integer, Stats>();
        private int fieldOrd;
        private int fieldVal;
        private Stats stats;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	
        	fieldScales = Utility.assertIntPairListConfigParam(config, "field.weights", fieldDelimRegex, ":", 
        			"missing numeric field scales");
        	for (Pair<Integer, Integer> fieldScale : fieldScales) {
        		fieldStats.put(fieldScale.getLeft(), new Stats());
        	}
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	//reduce will the stats first and then the data rows
            outKey.initialize();
            outKey.add(0, STATS_KEY);
        	for (int ord : fieldStats.keySet()) {
        		outVal.initialize();
        		outVal.add(ord);
        		fieldStats.get(ord).process().toTuple(outVal);
    			context.write(outKey, outVal);
        	}
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            outKey.initialize();
            outKey.add(1, items[ID_ORD]);
            
            outVal.initialize();
            for (int i = 1; i < items.length; ++i) {
            	fieldOrd = i;
            	stats = fieldStats.get(fieldOrd);
            	if (null != stats) {
            		//numeric
            		stats.add(Double.parseDouble(items[fieldOrd]));
            	} 
            	outVal.add(items[fieldOrd]);
            	
            }
			context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class NormalizerReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
        private List<Pair<Integer, Integer>> fieldScales;
        private Map<Integer, String> fieldTypes;
        private String normalizingStrategy;
        private float outlierTruncationLevel;
        private Map<Integer, Stats> fieldStats = new HashMap<Integer, Stats>();
        private int fieldOrd;
        private Stats stats;
        private int scale;
        private boolean excluded;
        private double normalizedValue;
        private int precision;
		private StringBuilder stBld = new StringBuilder();
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	
        	//filed scales
        	fieldScales = Utility.assertIntPairListConfigParam(config, "field.weights", fieldDelim, ":", 
        			"missing numeric field scales");

        	//field types
        	fieldTypes = Utility.assertIntegerStringMapConfigParam(config, "field.types", fieldDelim, ":", 
        			"missing numeric field types");
        	precision = config.getInt("floating.precision", 3);
        	
        	normalizingStrategy = config.get("normalizing.strategy", "minmax");
        	outlierTruncationLevel = config.getFloat("outlier.truncation.level", (float)-1.0);
        	for (Pair<Integer, Integer> fieldScale : fieldScales) {
        		stats = new Stats();
        		stats.scale = fieldScale.getRight();
        		fieldStats.put(fieldScale.getLeft(), stats);
        	}
        }
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		if (key.getInt(0) == 0) {
    			//aggregate stats
	        	for (Tuple value : values){
	        		fieldOrd = value.getInt(0);
	        		stats = new Stats();
	        		stats.fromTuple(value);
	        		fieldStats.get(fieldOrd).aggregate(stats);
	        	}
	        	
	        	//process stats
	        	for (int ord : fieldStats.keySet()) {
	        		fieldStats.get(ord).process();
	        	}
    		} else {
    			//records
        		stBld.delete(0, stBld.length());
	        	for (Tuple value : values){
	        		excluded = false;
	        		for (int i = 0; i < value.getSize(); ++i) {
	        			excluded = false;
	        			stBld.append(key.getString(1));
	        			stats= fieldStats.get(i);
	        			if (null != stats) {
	        				//numeric
	        				normalize(Double.parseDouble(value.getString(i)), stats);
	        				if (excluded) {
	        					break;
	        				} else {
	        					//other
	        					stBld.append(fieldDelim).append(formattedTypedValue(i));
	        				}
	        			} else {
        					//other types
        					stBld.append(fieldDelim).append(value.get(i));
	        			}
	        		}
	        		
	        		if (!excluded) {
	        			outVal.set(stBld.toString());
	        			context.write(NullWritable.get(), outVal);
	        		}
	        	}    	
    		}
    	}
    	
    	/**
    	 * @param value
    	 * @param stat
    	 * @param scale
    	 * @return
    	 */
    	private void normalize(double value, Stats stats) {
    		normalizedValue = 0;
    		if (normalizingStrategy.equals("minmax")) {
    			normalizedValue = ((value - stats.min) * stats.scale) / stats.range;
    		} else if (normalizingStrategy.equals("zscore")) {
    			double temp = (value - stats.mean) / stats.stdDev;
    			if (outlierTruncationLevel > 0) {
    				if (Math.abs(temp) > outlierTruncationLevel) {
    					excluded = true;
    				} else {
    					//keep bounded between -.5 * scale and .5 * scale
    					temp /= outlierTruncationLevel;
    				}
    			}
    			normalizedValue = temp * stats.scale / 2;
    		} else {
    			throw new IllegalStateException("invalid normalization strategy");
    		}
    	}
    	
    	/**
    	 * @param ord
    	 * @return
    	 */
    	private String formattedTypedValue(int ord) {
    		String value = null;
    		if (fieldTypes.get(0).equals("int")) {
    			int iValue = (int)normalizedValue;
    			value = "" + iValue;
    		} else if (fieldTypes.get(0).equals("long")) {
    			long lValue = (long)normalizedValue;
    			value = "" + lValue;
    		} else if (fieldTypes.get(0).equals("double")) {
    			value = Utility.formatDouble(normalizedValue, precision);
    		}
    		
    		return value;
    	}
    }
    
    /**
     * @author pranab
     *
     */
    private static class Stats {
    	private int count = 0;
    	private double min = Double.MAX_VALUE;
    	private double max = Double.MIN_VALUE;
    	private double sum = 0;
    	private double sqSum = 0;
    	private double mean;
    	private double range;
    	private double stdDev;
    	private int scale;
    	
    	/**
    	 * @param val
    	 */
    	private void add(double val) {
    		++count;
    		if (val < min) {
    			min = val;
    		}
    		if (max > val) {
    			max = val;
    		}
    		sum += val;
    		sqSum += val * val;
    	}
    	
    	/**
    	 * @param tuple
    	 */
    	private void toTuple(Tuple tuple) {
    		tuple.add(count, min, max, sum, sqSum);
    	}

    	/**
    	 * @param tuple
    	 */
    	private void fromTuple(Tuple tuple) {
    		count = tuple.getInt(1);
    		min = tuple.getInt(2);
    		max = tuple.getInt(3);
    		sum = tuple.getLong(4);
    		sqSum = tuple.getLong(5);
    	}
    	
    	/**
    	 * @param that
    	 */
    	private void aggregate(Stats that) {
    		count += that.count;
    		if (that.min < min) {
    			min = that.min;
    		}
    		if (that.max > max) {
    			max = that.max;
    		}
    		sum += that.sum;
    		sqSum += that.sqSum;
    	}
    	
    	/**
    	 * @return
    	 */
    	private Stats process() {
    		mean = sum / count;
    		range = max - min;
    		double temp = sqSum / count - mean * mean;
    		stdDev = Math.sqrt(temp);
    		return this;
    	}
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Normalizer(), args);
        System.exit(exitCode);
	}

}
