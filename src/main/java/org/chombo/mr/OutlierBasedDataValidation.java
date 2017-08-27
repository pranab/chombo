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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.stats.LongRunningStats;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Std deviation based based outlier detection for multiple quant field
 * @author pranab
 *
 */
public class OutlierBasedDataValidation extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Detecting invalid data as outliers";
        job.setJobName(jobName);
        
        job.setJarByClass(OutlierBasedDataValidation.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(OutlierBasedDataValidation.DataValidatorMapper.class);
        job.setReducerClass(OutlierBasedDataValidation.DataValidatorReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        int numReducer = job.getConfiguration().getInt("obdv.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class DataValidatorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] quantityAttrOrdinals;
        private boolean isAggrFileSplit;
        private int[] idFieldOrdinals;
        private int statOrd;
        private static final int PER_FIELD_STAT_VAR_COUNT = 6;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String value = Utility.assertConfigParam( config, "obdv.quantity.attr.ordinals", 
        			"quantity field ordinals must be provided");
        	quantityAttrOrdinals = Utility.intArrayFromString(value);
    		
        	String incrFilePrefix = Utility.assertConfigParam( config, "obdv.incremental.file.prefix", 
        			"Incremental file prefix needs to be specified");
        	isAggrFileSplit = !((FileSplit)context.getInputSplit()).getPath().getName().startsWith(incrFilePrefix);
        	
        	value = Utility.assertConfigParam( config, "obdv.id.field.ordinals", "ID field ordinals must be provided");
        	idFieldOrdinals = Utility.intArrayFromString(value);
       }
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	outKey.initialize();
        	outVal.initialize();
        	
        	if (isAggrFileSplit) {
        		for (int i = 0; i < idFieldOrdinals.length; ++i) {
      				outKey.append(items[i]);
        		}
        		outKey.append(0);
        		
        		//stat fields start after id fields
    			statOrd = idFieldOrdinals.length;
    			
    			for ( int ord : quantityAttrOrdinals) {
        			//existing aggregation - quantity attrubute ordinal, count, avg, std dev
                    outVal.add(0, Integer.parseInt(items[statOrd]), Long.parseLong(items[statOrd+1]), Long.parseLong(items[statOrd+4]) ,  
                    		Double.parseDouble(items[statOrd + 5]));
                    statOrd += PER_FIELD_STAT_VAR_COUNT;
    			}
        	} else {
        		//incremental - whole record
      			for (int ord : idFieldOrdinals ) {
      				outKey.append(items[ord]);
      			}
        		outKey.append(1);
        		
        		outVal.add(1);
        		for (String item : items) {
        			outVal.add(item);
        		}
        	}
        	context.write(outKey, outVal);
        }
 	}	

	   /**
	  *	 @author pranab
	  *
	 */
	public static class  DataValidatorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private  String fieldDelim;
		private int ord;
		private long avg;
		private double stdDev;
		private int[] quantityAttrOrdinals;
		private int index;
		private int recType;
		private String[] record;
		private float maxZscore;
		private float chebyshevStdDevMult;
		private float stdDevMult;
	    private Map<Integer, LongRunningStats> runningStats = new HashMap<Integer, LongRunningStats>();
	    private long min;
	    private long max;
	    private long delta;
	    private long count;
	    private boolean valid;
	    private long fieldValue;
	    private String outputType;
	    private boolean toOutput;
	    private String stVal;
	    private List<Integer> invalidFields = new ArrayList<Integer>();
	    private LongRunningStats stat;
	    private int minCountForStat;
	    private static final Logger LOG = Logger.getLogger(OutlierBasedDataValidation.DataValidatorReducer.class);	       
	    
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }
			fieldDelim = config.get("field.delim.out", ",");
			quantityAttrOrdinals = Utility.intArrayFromString(config.get("obdv.quantity.attr.ordinals"));
			maxZscore = config.getFloat("obdv.max.zscore", (float)-1.0);
			if (maxZscore  <  0) {
				double chebyshevIneqalityProb  =  config.getFloat("obdv.min.chebyshev.ineqality.prob", (float)-1.0);
				if (chebyshevIneqalityProb < 0) {
					throw new IllegalArgumentException("Either z score or chebyshev inequality probability must be provided");
				}
				chebyshevStdDevMult = (float)(Math.sqrt(1.0 / chebyshevIneqalityProb));
			}
			stdDevMult = maxZscore > 0 ? maxZscore  : chebyshevStdDevMult;

			outputType = config.get("obdv.output.type", "invalid");
			minCountForStat = config.getInt("obdv.min.count.for.stat", 2);
		}
		
 	/* (non-Javadoc)
 	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
 	 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			record = null;
			runningStats.clear();
			invalidFields.clear();
			for (Tuple val : values) {
				index = 0;
				recType = val.getInt(index++);
 			
				//all quant fields
				if (recType == 0) {
					//aggregate with stats
					for ( int quantOrd : quantityAttrOrdinals) {
						ord = val.getInt(index++);
						count = val.getLong(index++);
						avg = val.getLong(index++);
						stdDev  = val.getDouble(index++);
						if (count >= minCountForStat) {
							runningStats.put(ord, new LongRunningStats(ord, avg, stdDev));
						}
					} 
				} else {
					//record
					record = val.subTupleAsArray(1);
				}
			}
 		
			if (null != record) {
				valid = true;
				for ( int quantOrd : quantityAttrOrdinals) {
    				stat = runningStats.get(quantOrd);
    				if (null == stat) {
    					valid = true;
    				} else {
	    				delta = Math.round(stat.getStdDev() * stdDevMult);
	    				min = stat.getAvg() -  delta;
	    				max = stat.getAvg() +  delta;
	    				fieldValue = Long.parseLong(record[quantOrd]);
	    				valid = fieldValue >= min && fieldValue <= max;
    				}
    				if (!valid) {
    					invalidFields.add(quantOrd);
    					context.getCounter("Data quality", "invalid attribute").increment(1);
	    				LOG.debug( "invalid:  " + record[0] + "," + record[2] + " fieldValue: " + fieldValue + " min: " + min + " max: " + max );
    				}
				}

				valid = invalidFields.isEmpty();
				if (!valid) {
					context.getCounter("Data quality", "invalid record").increment(1);
				}
				toOutput = outputType.equals("valid") && valid || outputType.equals("invalid") && !valid ||
						outputType.equals("all");
				if (toOutput) {
					stVal = Utility.join(record);
					
					//append invalid field ordinals
					if (outputType.equals("all")) {
						stVal = stVal + fieldDelim + Utility.join(invalidFields, ":");
					}
					outVal.set(stVal);
					context.write(NullWritable.get(), outVal);
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new OutlierBasedDataValidation(), args);
		System.exit(exitCode);
	}
	
}
