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
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class SeasonalAggregator  extends Configured implements Tool {
	private static String configDelim = ",";
    private static final String AGGR_COUNT = "count";
    private static final String AGGR_SUM = "sum";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Seasonal aggregator  for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(SeasonalAggregator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(SeasonalAggregator.AggregatorMapper.class);
        job.setReducerClass(SeasonalAggregator.AggregateReducer.class);
        job.setCombinerClass(SeasonalAggregator.AggregateCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("seg.num.reducer", -1);
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
        private static final String  QUARTER_HOUR_OF_DAY = "quarterHourOfDay";
        private static final String  HALF_HOUR_OF_DAY = "halfHourOfDay";
        private static final String  HOUR_OF_DAY = "hourOfDay";
        private static final String  DAY_OF_WEEK  = "dayOfWeek";
        private static long secInWeek =7L * 24 * 60 * 60;
        private static long secInDay =24L * 60 * 60;
        private static long secInHour = 60L * 60;
        private static long secInHalfHour = 30L * 60;
        private static long secInQuarterHour = 15L * 60;
        private long parentCycleIndex;
        private int cycleIndex;
        private long timeStamp;
        private String aggregatorType;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	attributes = Utility.intArrayFromString(config.get("quant.attr.list"),fieldDelimRegex );
        	idOrdinals = Utility.intArrayFromString(config.get("id.field.ordinals"), configDelim);
        	timeStampFieldOrdinal = config.getInt("time.stamp.field.ordinal", -1);
        	seasonalCycleType = config.get("seasonal.cycle.type");
        	aggregatorType = config.get("aggregator.type");
       }
		
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            
            timeStamp = Long.parseLong(items[timeStampFieldOrdinal]);
            getCycleIndex(timeStamp);
            
        	for (int attr : attributes) {
            	outKey.initialize();
            	outVal.initialize();
            	addIdstoKey();
            	outKey.add(parentCycleIndex, cycleIndex, attr);
            	
            	if (aggregatorType.equals(AGGR_COUNT)) {
            		outVal.add(1);
            	} else {
            		outVal.add(Double.parseDouble(items[attr]));
            	}
        	}
        	
        }       
        
        private void addIdstoKey() {
        	if (null != idOrdinals) {
        		for (int ord  :  idOrdinals) {
        			outKey.add(items[ord]);
        		}
        	}
        }
 
        /**
         * @param timeStamp
         */
        private void  getCycleIndex(long timeStamp) {
        	if (seasonalCycleType.equals(DAY_OF_WEEK)) {
            	parentCycleIndex = timeStamp / secInWeek;
        		cycleIndex = (int)((timeStamp % secInWeek) / secInDay);
        	} else if (seasonalCycleType.equals(HOUR_OF_DAY)) {
            	parentCycleIndex = timeStamp / secInDay;
        		cycleIndex = (int)((timeStamp % secInDay) / secInHour);
        	}  else  if (seasonalCycleType.equals(HALF_HOUR_OF_DAY)) {
            	parentCycleIndex = timeStamp / secInDay;
        		cycleIndex = (int)((timeStamp % secInDay) / secInHalfHour);
        	} else  if (seasonalCycleType.equals(QUARTER_HOUR_OF_DAY)) {
            	parentCycleIndex = timeStamp / secInDay;
        		cycleIndex = (int)((timeStamp % secInDay) / secInQuarterHour);
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

        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	aggregatorType = config.get("aggregator.type");
        }
        
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		sum = 0;
    		totalCount = 0;
    		for (Tuple val : values) {
            	if (aggregatorType.equals(AGGR_COUNT)) {
            		totalCount += val.getInt(0);
            	} else {
            		sum  += val.getDouble(0);
            	}
    		}
        
    		outVal.initialize();
    		if (aggregatorType.equals(AGGR_COUNT)) {
    			outVal.add(totalCount);
    		} else {
    			outVal.add(sum);
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
		private String aggregatorType;
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
           	aggregatorType = config.get("aggregator.type");
		}

        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		sum = 0;
    		totalCount = 0;
    		for (Tuple val : values) {
            	if (aggregatorType.equals(AGGR_COUNT)) {
            		totalCount += val.getInt(0);
            	} else {
            		sum  += val.getDouble(0);
            	}
    		}
    		
    		int keySize = key.getSize();
        	if (aggregatorType.equals(AGGR_COUNT)) {
        		outVal.set(key.toString(0, keySize -3) +fieldDelim +  key.toString(keySize -2, keySize) + totalCount);
        	} else {
        		outVal.set(key.toString(0, keySize -3) +fieldDelim +  key.toString(keySize -2, keySize) + sum);
        	}
        	context.write(NullWritable.get(), outVal);
        }		
 	}
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SeasonalAggregator(), args);
		System.exit(exitCode);
	}
 
}
