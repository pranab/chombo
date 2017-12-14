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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.BasicUtils;
import org.chombo.util.Triplet;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Does data normalization. Can use minmax or zscore normalization. With zscore
 * normalization, additionally outlier can be removed
 * @author pranab
 *
 */
public class Normalizer extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Normalizer.class);

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
        private int[] numAttributes;
        private Map<Integer, Triplet<String, Integer, String>> attributeProperties = 
        		new HashMap<Integer, Triplet<String, Integer, String>>();
        private static final int ID_ORD = 0;
        private static final String STATS_KEY = "stats";
        private Map<Integer, Stats> fieldStats = new HashMap<Integer, Stats>();
        private int fieldOrd;
        private Stats stats;
        private String idStrategy;
        private int[] idOrdinals;
        private final String ID_STARTEGY_DEFAULT = "default";
        private final String ID_STARTEGY_SPECIFIED = "specified";
        private final String ID_STARTEGY_GENERATE = "generate";
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "nor.field.delim.regex", "field.delim.regex", ",");
        	
        	numAttributes = Utility.assertIntArrayConfigParam(config, "nor.num.attribute.ordinals", fieldDelimRegex, 
        			"missing numerical attribute ordinals");
        	getAttributeProperties(numAttributes,attributeProperties, config);
        	
        	//initialize stats
        	for (int ord : attributeProperties.keySet()) {
        		fieldStats.put(ord, new Stats());
        	}
        	
        	//record ID
        	idStrategy = config.get("nor.id.strategy", ID_STARTEGY_DEFAULT);
        	if (idStrategy.equals(ID_STARTEGY_SPECIFIED)) {
            	idOrdinals = Utility.assertIntArrayConfigParam(config, "nor.id.field.ordinals", Utility.configDelim, 
            			"missing id field ordinals");
        	}
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	//reducer will process the stats first and then the data rows
            outKey.initialize();
            outKey.add(0, STATS_KEY);
            
        	for (int ord : fieldStats.keySet()) {
        		outVal.initialize();
        		outVal.add(ord);
        		fieldStats.get(ord).toTuple(outVal);
    			context.write(outKey, outVal);
        	}
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            outKey.initialize();
            outKey.add(1, getKey());
            
            outVal.initialize();
            for (int i = 0; i < items.length; ++i) {
            	fieldOrd = i;
            	stats = fieldStats.get(fieldOrd);
            	if (null != stats) {
            		//configured numeric fields only
            		stats.add(Double.parseDouble(items[fieldOrd]));
            	} 
            	outVal.add(items[fieldOrd]);
            }
            
			context.write(outKey, outVal);
        }
        
        /**
         * @return
         */
        private String getKey() {
        	String key = null;
        	if (idStrategy.equals(ID_STARTEGY_DEFAULT)) {
        		key = items[ID_ORD];
        	} else if (idStrategy.equals(ID_STARTEGY_SPECIFIED)) {
        		key = BasicUtils.extractFields(items, idOrdinals, fieldDelimRegex);
        	} else if (idStrategy.equals(ID_STARTEGY_GENERATE)) {
        		key = BasicUtils.generateId();
        	} else {
        		throw new IllegalStateException("invalid record Id strtaegy");
        	}
        	
        	return key;
        }
        
	}
	
    /**
     * @author pranab
     *
     */
    public static class NormalizerReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
        private int[] numAttributes;
        private Map<Integer, Triplet<String, Integer, String>> attributeProperties = 
        		new HashMap<Integer, Triplet<String, Integer, String>>();
        private String normalizingStrategy;
        private float outlierTruncationLevel;
        private Map<Integer, Stats> fieldStats = new HashMap<Integer, Stats>();
        private int fieldOrd;
        private Stats stats;
        private boolean excluded;
        private double normalizedValue;
        private int precision;
        private int ordinal;
		private StringBuilder stBld = new StringBuilder();
		private boolean forceUnitRange;
		private static final String NORM_MIN_MAX = "minmax";
		private static final String NORM_MAX = "max";
		private static final String NORM_ZSCORE = "zscore";
		private static final String NORM_CENTER = "center";
		private static final String NORM_DECIMAL = "decimal";
		private static final String NORM_UNIT_SUM = "unitSum";
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
            fieldDelim = config.get("field.delim.out", ",");
        	
        	//attribute properties
        	numAttributes = Utility.assertIntArrayConfigParam(config, "nor.num.attribute.ordinals", Utility.configDelim, 
        			"missing numerical attribute ordinals");
        	getAttributeProperties(numAttributes,attributeProperties, config);
        	
        	precision = config.getInt("nor.floating.precision", 3);
        	normalizingStrategy = config.get("nor.normalizing.strategy", NORM_MIN_MAX);
        	outlierTruncationLevel = config.getFloat("nor.outlier.truncation.level", (float)-1.0);
        	for (int ord : attributeProperties.keySet()) {
        		Triplet<String, Integer, String> attrProp = attributeProperties.get(ord);
        		stats = new Stats();
        		stats.scale = attrProp.getCenter();
        		stats.transformer = attrProp.getRight();
        		fieldStats.put(ord, stats);
        	}
        	
        	//for z score normalized force e most values to have unit range
        	forceUnitRange = config.getBoolean("nor.force.unit.range", true);
        }
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
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
	        		Stats stats = fieldStats.get(ord);
	        		stats.process();
	        		//System.out.println("ord:" + ord + " min:" + stats.min + " max:" + stats.max);
	        	}
    		} else {
    			//records
	        	for (Tuple value : values){
	        		stBld.delete(0, stBld.length());
	        		excluded = false;
	        		
	        		//all fields
	        		for (int i = 0; i < value.getSize(); ++i) {
	        			excluded = false;
	        			ordinal = i;
	        			stats= fieldStats.get(ordinal);
	        			if (null != stats) {
	        				//numeric
	        				normalize(Double.parseDouble(value.getString(i)), stats);
	        				if (excluded) {
	        					break;
	        				} else {
	        					stBld.append(formattedTypedValue(ordinal)).append(fieldDelim);
	        				}
	        			} else {
        					//pass through for other types
        					stBld.append(value.getString(i)).append(fieldDelim);
	        			}
	        		}
	        		
	        		if (!excluded) {
	        			outVal.set(stBld.substring(0, stBld.length() -1));
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
    		//transform
    		value = stats.transform(value);
    		
    		//normalize
    		normalizedValue = 0;
    		if (normalizingStrategy.equals(NORM_MIN_MAX)) {
    			normalizedValue = ((value - stats.min) * stats.scale) / stats.range;
    		} else if (normalizingStrategy.equals(NORM_MAX)) { 
    			double  absMax = BasicUtils.max(Math.abs(stats.max), Math.abs(stats.min));
    			normalizedValue = value / absMax;
    		} else if (normalizingStrategy.equals(NORM_CENTER)) {
    			normalizedValue = (value - stats.mean) * stats.scale;
    		} else if (normalizingStrategy.equals(NORM_DECIMAL)) { 
    			double  absMax = BasicUtils.max(Math.abs(stats.max), Math.abs(stats.min));
    			double maxLog = Math.log10(absMax);
    			int pwr = (int)(maxLog + 1);
    			normalizedValue = value / Math.pow(10, pwr);
    		} else if (normalizingStrategy.equals(NORM_UNIT_SUM)) {
    			normalizedValue = (value / stats.sum) * stats.scale;
    		} else if (normalizingStrategy.equals(NORM_ZSCORE)) {
    			if (stats.gotTransformer()) {
    				throw new IllegalStateException("can not apply zscore normalizer when data is transformed");
    			}
    			double temp = (value - stats.mean) / stats.stdDev;
    			if (outlierTruncationLevel > 0) {
    				if (Math.abs(temp) > outlierTruncationLevel) {
    					//exclude outlier
    					excluded = true;
    				} else {
    					//keep bounded between -.5 * scale and .5 * scale
    					temp /= outlierTruncationLevel;
    				}
    			}
    			
				normalizedValue = temp * stats.scale;
				
    			//force most values between +0.5 and -0.5
    			if (forceUnitRange) {
    				normalizedValue /= 2;
    			}
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
    		Triplet<String, Integer, String> attrProp = attributeProperties.get(ord);
    		String dataType = attrProp.getLeft();
    		if (dataType.equals("int")) {
    			int iValue = (int)normalizedValue;
    			value = "" + iValue;
    		} else if (dataType.equals("long")) {
    			long lValue = (long)normalizedValue;
    			value = "" + lValue;
    		} else if (dataType.equals("double")) {
    			value = BasicUtils.formatDouble(normalizedValue, precision);
    		} else {
    			throw new IllegalStateException("invalid numeric data types");
    		}
    		
    		return value;
    	}
    }
    
    /**
     * @param numAttributes
     * @param attributeProperties
     * @param config
     */
    private static void getAttributeProperties(int[] numAttributes, 
    		Map<Integer, Triplet<String, Integer, String>> attributeProperties, Configuration config) {
    	for (int i : numAttributes) {
    		String key = "nor.attribute.prop." + i;
    		String value = config.get(key);
    		if (null == value) {
    			throw new IllegalStateException("missing attribute properties");
    		}
    		String[] parts = value.split(Utility.configSubFieldDelim);
    		Triplet<String, Integer, String> attributeProp = null;
    		if (parts.length == 2) {
    			//data type, scale
    			attributeProp = new Triplet<String, Integer, String>(parts[0], Integer.parseInt(parts[1]), "none");
    		} else if (parts.length == 3) {
    			//data type, scale, transformer
    			attributeProp = new Triplet<String, Integer, String>(parts[0], Integer.parseInt(parts[1]), parts[2]);
    		} else {
    			throw new IllegalStateException("invalid attribute properties format");
    		}
    		attributeProperties.put(i, attributeProp);
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
    	private String transformer;
    	
    	/**
    	 * @param val
    	 */
    	private void add(double val) {
    		++count;
    		if (val < min) {
    			min = val;
    		}
    		if (val > max) {
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
    		min = tuple.getDouble(2);
    		max = tuple.getDouble(3);
    		sum = tuple.getDouble(4);
    		sqSum = tuple.getDouble(5);
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
    		
    		if (transformer.equals("multiplicativeInverse")) {
    			//update min max and range
    			double tempMin = min;
    			min = 1.0 / max;
    			max = 1.0 / tempMin;
    			range = max - min;
    		}
    		
    		return this;
    	}
    	
    	/**
    	 * @param value
    	 * @return
    	 */
    	private double transform(double value) {
    		double newValue = 0;
    		if (transformer.equals("additiveInverse")) {
    			newValue = max - value;
    		} else if (transformer.equals("multiplicativeInverse")) {
    			newValue = 1.0 / value;
    		} else if (transformer.equals("none")) {
    			newValue = value;
    		} else {
    			throw new IllegalStateException("invalid data transformer");
    		}
    		return newValue;
    	}
    	
    	/**
    	 * @return
    	 */
    	private boolean gotTransformer() {
    		return !transformer.equals("none");
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
