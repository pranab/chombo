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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.stats.LongRunningStats;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;


/**
 * Calculates running average and other stats of some quantity
 * @author pranab
 *
 */
public class RunningAggregator  extends Configured implements Tool {
    private static int REC_TYPE_AGGR = 0;
    private static int REC_TYPE_INCR = 1;
    
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Running aggregates  for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(RunningAggregator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(RunningAggregator.AggrMapper.class);
        job.setReducerClass(RunningAggregator.AggrReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("rug.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class AggrMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] quantityAttrOrdinals;
        private boolean isAggrFileSplit;
        private int[] idFieldOrdinals;
        private long newValue;
        private int statOrd;
        private static final int PER_FIELD_STAT_VAR_COUNT = 6;
        private static final Logger LOG = Logger.getLogger(RunningAggregator.AggrMapper.class);

        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }

            //quant fields
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	quantityAttrOrdinals = Utility.assertIntArrayConfigParam(config, "rug.quantity.attr.ordinals", 
        			Utility.configDelim, "quantity field ordinals must be provided");
        	
        	String aggrFilePrefix = config.get("rug.aggregate.file.prefix", "");
        	if (!aggrFilePrefix.isEmpty()) {
        		isAggrFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(aggrFilePrefix);
        	} else {
            	String incrFilePrefix =Utility.assertConfigParam(config, "rug.incremental.file.prefix", 
            			"Incremental file prefix needs to be specified");
            	isAggrFileSplit = !((FileSplit)context.getInputSplit()).getPath().getName().startsWith(incrFilePrefix);
        	}
        	
        	//ID fields
        	idFieldOrdinals = Utility.assertIntArrayConfigParam(config, "rug.id.field.ordinals", 
        			Utility.configDelim, "ID field ordinals must be provided");
       }
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	
        	if (isAggrFileSplit) {
        		//aggregate data
        		initKey();
    			statOrd = idFieldOrdinals.length;
        		int quantAttrOrd = Integer.parseInt(items[statOrd]);
        		outKey.append(quantAttrOrd);
        		
    			//existing aggregation - quantity attrubute ordinal, count, sum, sum of squares
            	outVal.initialize();
                outVal.add(REC_TYPE_AGGR, quantAttrOrd, Long.parseLong(items[statOrd+1]) ,  
                		Long.parseLong(items[statOrd + 2]),  Long.parseLong(items[statOrd + 3]));
                context.write(outKey, outVal);
        	} else {
            	//incremental - first run will have only incremental file
    			for (int ord : quantityAttrOrdinals) {
    				//emit one for each quant field
            		initKey();
            		outKey.append(ord);

                	outVal.initialize();
	        		newValue = Long.parseLong( items[ord]);
	                outVal.add(REC_TYPE_INCR, ord, (long)1, newValue, newValue * newValue);
	               	context.write(outKey, outVal);
	            }
        	}
        }
        
        /**
         * 
         */
        private void initKey() {
        	outKey.initialize();
    		for (int indx : idFieldOrdinals) {
  				outKey.append(items[indx]);
    		}
        }
 	}	

	   /**
  * @author pranab
  *
  */
 public static class AggrReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		private Text outVal = new Text();
 		private StringBuilder stBld =  new StringBuilder();;
 		private  String fieldDelim;
 		private int ord;
		private long sum;
		private long count;
		private long sumSq;
        private int index;
        private Map<Integer, LongRunningStats> runningStats = new HashMap<Integer, LongRunningStats>();
        private boolean  handleMissingIncremental;
        private int recType;
        private int recCount;
        private Set<Integer> recTypes = new HashSet<Integer>();
        private OutputStream aggrStrm;
        private PrintWriter aggrWriter;
        private static final Logger LOG = Logger.getLogger(RunningAggregator.AggrReducer.class);
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }
        	fieldDelim = config.get("field.delim.out", ",");
        	handleMissingIncremental = config.getBoolean("rug.handle.missing.incremental",  false);
        	
        	//only modified aggregate data output
        	String aggrFilePath = config.get("rug.mod.aggr.file.path");
        	if (null != aggrFilePath) {
        		aggrStrm = Utility.getCreateFileOutputStream(config,aggrFilePath);
        		aggrWriter = new PrintWriter(aggrStrm);
        	}
        }
		
        @Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			super.cleanup(context);
			if (null != aggrStrm) {
				aggrWriter.close();
				aggrStrm.close();
			}
        }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
    		runningStats.clear();
    		recCount = 0;
    		recTypes.clear();
    		for (Tuple val : values) {
    			index = 0;
    			recType = val.getInt(index++);
    			recTypes.add(recType);
    			
				ord = val.getInt(index++);
				count = val.getLong(index++);
				sum  = val.getLong(index++);
				sumSq  = val.getLong(index++);
				
				LongRunningStats stat = runningStats.get(ord);
				if (null == stat) {
					//create stat object
					runningStats.put(ord, new LongRunningStats(ord, count, sum, sumSq));
				} else {
					//update stat
					stat.accumulate(count, sum, sumSq);
				}
    			++recCount;
    		}   	
    		
    		//if only aggregate record, increment count by 1
    		if (handleMissingIncremental && recCount == 1 && recType == REC_TYPE_AGGR) {
    			LongRunningStats stat = runningStats.get(ord);
    			stat.incrementCount();
    		}
    		
    		//output
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString()).append(fieldDelim);
			LongRunningStats stat = runningStats.get(ord);
			stat.process();
			stBld.append(stat.getCount()).append(fieldDelim).
				append(stat.getSum()).append(fieldDelim).append(stat.getSumSq()).append(fieldDelim).
				append(stat.getAvg()).append(fieldDelim).append(stat.getStdDev());
    		
			String outStr = stBld.toString();
        	outVal.set(outStr);
			context.write(NullWritable.get(), outVal);
			
			//output only updated aggregates if aggregate output file path specified
			if (recTypes.size() == 2 && null != aggrWriter) {
				aggrWriter.write(stBld.append("\n").toString());
			}
    	}
		
 	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RunningAggregator(), args);
		System.exit(exitCode);
	}
 
}
