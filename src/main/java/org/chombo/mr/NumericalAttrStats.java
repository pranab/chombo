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
import org.chombo.util.Attribute;
import org.chombo.util.AttributeZscoreFilter;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.SeasonalAnalyzer;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Basic stats for numerical attributes
 * @author pranab
 *
 */
public class NumericalAttrStats  extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Basic stats for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericalAttrStats.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(NumericalAttrStats.StatsMapper.class);
        job.setReducerClass(NumericalAttrStats.StatsReducer.class);
        job.setCombinerClass(NumericalAttrStats.StatsCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("nas.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class StatsMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private double val;
        private double sqVal;
        private int count = 1;
        private String[] items;
        private int[] idOrdinals;
        private GenericAttributeSchema schema;
        private boolean seasonalAnalysis;
        private String seasonalCycleType;
        private int timeStampFieldOrdinal;
        private SeasonalAnalyzer seasonalAnalyzer;
        private long timeStamp;
        private int cycleIndex;
        private AttributeZscoreFilter attrZscoreFilter;
        
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "nas.schema.file.path");
        	attributes =  Utility.getAttributes("nas.attr.list", configDelim,schema, config,  
        			Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_DOUBLE);        	
        	
        	idOrdinals = Utility.intArrayFromString(config.get("nas.id.field.ordinals"), configDelim);
        	
        	//seasonal
        	seasonalAnalysis = config.getBoolean("nas.seasonal.analysis", false);
        	if (seasonalAnalysis) {
        		seasonalCycleType =  Utility.assertStringConfigParam(config,"nas.seasonal.cycle.type", 
        				"missing seasonal cycle type parameter");
        		seasonalAnalyzer = new SeasonalAnalyzer(seasonalCycleType);
            	if (seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_DAY ) ||  
            			seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_END_DAY ) ) {
            		Map<Integer, Integer>  hourRanges = Utility. assertIntegerIntegerMapConfigParam(config, "nas.seasonal.hour.groups", 
            				Utility.configDelim, Utility.configSubFieldDelim, "missing hour groups", true);
            		seasonalAnalyzer.setHourRanges(hourRanges);
            	} 
            	
            	int  timeZoneShiftHours = config.getInt("nas.time.zone.hours",  0);
            	if (timeZoneShiftHours > 0) {
            		seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours);
            	}

            	timeStampFieldOrdinal = Utility.assertIntConfigParam(config,"nas.time.stamp.field.ordinal", 
            			"missing time stamp field ordinal"); 
            	boolean timeStampInMili = config.getBoolean("nas.time.stamp.in.mili", true);
            	seasonalAnalyzer.setTimeStampInMili(timeStampInMili);
        	}

        	//score filter
        	boolean filterOutlier = config.getBoolean("nas.filter.outlier", false);
        	if (filterOutlier) {
        		String fieldDelim = config.get("field.delim.out", ",");
        		Map<Integer, Double> attrZscores = Utility.assertIntegerDoubleMapConfigParam(config, "nas.attr.zscore", 
        				Utility.configDelim, Utility.configSubFieldDelim, "attribute max zscore missing");
        		attrZscoreFilter = new AttributeZscoreFilter(attrZscores, config, "nas.stats.file.path", fieldDelim);
        	}
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);

            //seasonality cycle index
    		if (seasonalAnalysis) {
                timeStamp = Long.parseLong(items[timeStampFieldOrdinal]);
                cycleIndex = seasonalAnalyzer.getCycleIndex(timeStamp);
                
                //outside seasonal time band
                if (cycleIndex < 0) {
                	return;
                }
    		}
            
         	for (int attr : attributes) {
            	outKey.initialize();
            	outVal.initialize();
            	
            	if (null != idOrdinals) {
            		outKey.addFromArray(items, idOrdinals);
            	}
        		
        		//seasonal analysis
        		if (seasonalAnalysis) {
                    outKey.add(cycleIndex);
        		}
        		outKey.add(attr);

            	val = Double.parseDouble(items[attr]);
            	if (null == attrZscoreFilter || attrZscoreFilter.isWithinBound(attr, val))  {
            		//emit if filter is not set or value is within zscore bounds 
                	sqVal = val * val;
                	outVal.add(val, val, val, sqVal, count);
                	context.write(outKey, outVal);
            	}
        	}
        }
	}

	/**
	 * @author pranab
	 *
	 */
	public static class StatsCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private double sum;
		private double sumSq;
		private int totalCount;
		private double min;
		private double max;
		private double curMin;
		private double curMax;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		sum = 0;
    		sumSq = 0;
    		totalCount = 0;
    		int i = 0;
    		for (Tuple val : values) {
    			sum  += val.getDouble(0);
    			curMin = val.getDouble(1);
    			curMax = val.getDouble(2);
    			sumSq += val.getDouble(3);
    			totalCount += val.getInt(4);
    			if (i == 0) {
    				min = curMin;
    				max = curMax;
    			} else {
    				if (curMin < min) {
    						min = curMin;
    				} else if (curMax > max) {
    					max = curMax;
    				}
    			}
    			++i;
    		}
    		outVal.initialize();
    		outVal.add(sum, min, max, sumSq, totalCount);
        	context.write(key, outVal);       	
        }		
	}	
	
   /**
     * @author pranab
     *
     */
    public static class StatsReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		protected double sum;
		protected double sumSq;
		protected int totalCount;
		protected double mean;
		protected double variance;
		protected double stdDev;
		protected double min;
		protected double max;
		private double curMin;
		private double curMax;
        private int[] idOrdinals;
        private int outputPrecision ;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	idOrdinals = Utility.intArrayFromString(config.get("nas.id.field.ordinals"), configDelim);
        	outputPrecision = config.getInt("nas.output.prec", 3);
     }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
    		processReduce(values);
    		emitOutput( key,  context);
    	}
    	
    	/**
    	 * @param values
    	 */
    	protected void processReduce(Iterable<Tuple> values) {
    		sum = 0;
    		sumSq = 0;
    		totalCount = 0;
    		int i = 0;
    		for (Tuple val : values) {
       			sum  += val.getDouble(0);
    			curMin = val.getDouble(1);
    			curMax = val.getDouble(2);
    			sumSq += val.getDouble(3);
    			totalCount += val.getInt(4);
    			if (i == 0) {
    				min = curMin;
    				max = curMax;
    			} else {
    				if (curMin < min) {
    					min = curMin;
    				} else if (curMax > max) {
    					max = curMax;
    				}
    			}
    			++i;
    		}

    		mean = sum / totalCount;
    		variance = sumSq / totalCount - mean * mean;
    		stdDev = Math.sqrt(variance);
    	}
    	
    	
    	/**
    	 * @param key
    	 * @param context
    	 * @throws IOException
    	 * @throws InterruptedException
    	 */
    	protected  void emitOutput(Tuple key,  Context context) throws IOException, InterruptedException {
    		//x)partitonIds, (0)attr ord (1)seasonal cycle (2)cond attr 3)sum 4)sum square 5)count 6)mean 7)variance 8)std dev 9)min 10)max 
    		stBld.delete(0, stBld.length());
        	stBld.append(key.toString()).append(fieldDelim);
        	
    		stBld.append(sum).append(fieldDelim).append(Utility.formatDouble(sumSq, outputPrecision)).
    			append(fieldDelim).append(totalCount).append(fieldDelim) ;
    		stBld.append(Utility.formatDouble(mean, outputPrecision)).append(fieldDelim).append(Utility.formatDouble(variance, outputPrecision)).
    			append(fieldDelim).append(Utility.formatDouble(stdDev, outputPrecision)).append(fieldDelim)  ;
    		stBld.append(Utility.formatDouble(min, outputPrecision)).append(fieldDelim).append(Utility.formatDouble(max, outputPrecision)) ;
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericalAttrStats(), args);
        System.exit(exitCode);
	}

}
