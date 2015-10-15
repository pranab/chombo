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
import org.chombo.util.SeasonalAnalyzer;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class SeasonalDetector  extends Configured implements Tool {
	private static String configDelim = ",";
    private static final String AGGR_COUNT = "count";
    private static final String AGGR_SUM = "sum";
    private static final String AGGR_AVG = "average";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Seasonal aggregator  for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(SeasonalDetector.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(SeasonalDetector.AggregatorMapper.class);
        job.setReducerClass(SeasonalDetector.AggregateReducer.class);
        job.setCombinerClass(SeasonalDetector.AggregateCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("sed.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class AggregatorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private String[] items;
        private int[] idOrdinals;
        private int timeStampFieldOrdinal;
        private String seasonalCycleType;
        private int cycleIndex;
        private long timeStamp;
        private String aggregatorType;
        private SeasonalAnalyzer seasonalAnalyzer;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	attributes = Utility.assertIntArrayConfigParam(config, "sed.quant.attr.list", fieldDelimRegex, "missing quant attribute list");
        	idOrdinals = Utility.intArrayFromString(config.get("sed.id.field.ordinals"), configDelim);
        	timeStampFieldOrdinal = Utility.assertIntConfigParam(config,"sed.time.stamp.field.ordinal", "missing timestamp field ordinal");
        	seasonalCycleType = Utility.assertStringConfigParam(config,"sed.seasonal.cycle.type", "missing seasonal cycle type");
        	aggregatorType = config.get("sed.aggregator.type", AGGR_AVG);
        	
    		seasonalAnalyzer = new SeasonalAnalyzer(seasonalCycleType);
        	if (seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_DAY ) ||  
        			seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_END_DAY ) ) {
        		Map<Integer, Integer>  hourRanges = Utility. assertIntIntegerIntegerMapConfigParam(config, "hour.groups", 
        				Utility.configDelim, Utility.configSubFieldDelim, "missing hour groups");
        		seasonalAnalyzer.setHourRanges(hourRanges);
        	} 
        	
        	int  timeZoneShiftHours = config.getInt("time.zone.shift.hours",  0);
        	if (timeZoneShiftHours > 0) {
        		seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours);
        	}
        	boolean timeStampInMili = config.getBoolean("time.stamp.in.mili", true);
        	seasonalAnalyzer.setTimeStampInMili(timeStampInMili);
       }
		
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            
            timeStamp = Long.parseLong(items[timeStampFieldOrdinal]);
            cycleIndex = seasonalAnalyzer.getCycleIndex(timeStamp);
            
            if (cycleIndex >= 0) {
	        	for (int attr : attributes) {
	            	outKey.initialize();
	            	outVal.initialize();
	            	
	            	if (null != idOrdinals) {
	            		outKey.addFromArray(items, idOrdinals);
	            	}
	            	outKey.add(seasonalAnalyzer.getParentCycleIndex(), cycleIndex, attr);
	            	
	            	if (aggregatorType.equals(AGGR_COUNT)) {
	            		outVal.add(1);
	            	} else if (aggregatorType.equals(AGGR_SUM)) {
	            		outVal.add(Double.parseDouble(items[attr]));
	            	} else if (aggregatorType.equals(AGGR_AVG)) {
	            		outVal.add(1, Double.parseDouble(items[attr]));
	            	} else {
	        			throw new IllegalArgumentException("invalid aggregation function");
	        		}
	            	context.write(outKey, outVal);
	        	}
            }
        }       
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class AggregateCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private int totalCount;
		private double sum;
		private String aggregatorType;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	aggregatorType = config.get("sed.aggregator.type");
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		sum = 0;
    		totalCount = 0;
    		for (Tuple val : values) {
            	if (aggregatorType.equals(AGGR_COUNT)) {
            		totalCount += val.getInt(0);
            	} else if (aggregatorType.equals(AGGR_SUM)) {
            		sum  += val.getDouble(0);
            	} else if (aggregatorType.equals(AGGR_AVG)) {
            		totalCount += val.getInt(0);
            		sum  += val.getDouble(1);
            	}
    		}
        
    		outVal.initialize();
    		if (aggregatorType.equals(AGGR_COUNT)) {
    			outVal.add(totalCount);
    		} else if (aggregatorType.equals(AGGR_SUM)) {
    			outVal.add(sum);
    		} else if (aggregatorType.equals(AGGR_AVG)) {
    			outVal.add(totalCount, sum);
    		} 
        	context.write(key, outVal);       	
        }
	}
	
	   /**
  * @author pranab
  *
  */
 public static class  AggregateReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		protected double sum;
		protected int totalCount;
		private double average;
		private String aggregatorType;
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
           	aggregatorType = config.get("sed.aggregator.type");
		}

        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		sum = 0;
    		totalCount = 0;
    		for (Tuple val : values) {
            	if (aggregatorType.equals(AGGR_COUNT)) {
            		totalCount += val.getInt(0);
            	} else if (aggregatorType.equals(AGGR_SUM)) {
            		sum  += val.getDouble(0);
            	}  else if (aggregatorType.equals(AGGR_AVG)) {
            		totalCount += val.getInt(0);
            		sum  += val.getDouble(1);
            	}
    		}
    		
        	if (aggregatorType.equals(AGGR_COUNT)) {
        		outVal.set(key.toString() + fieldDelim +  totalCount);
        	} else if (aggregatorType.equals(AGGR_SUM)) {
        		outVal.set(key.toString() +fieldDelim  + String.format("%.3f", sum));
        	} else if (aggregatorType.equals(AGGR_AVG)) {
        		average = sum / totalCount;
        		outVal.set(key.toString() +fieldDelim  + String.format("%.3f", average));
        	}
        	context.write(NullWritable.get(), outVal);
        }		
 	}
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SeasonalDetector(), args);
		System.exit(exitCode);
	}
 
}
