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
import java.util.Collections;
import java.util.List;

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
import org.chombo.stats.ValueCounter;
import org.chombo.stats.ValueCounters;
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts missing field values column wise or row wise
 * @author pranab
 *
 */
public class MissingValueCounter extends Configured implements Tool {
	private static String OP_ROW = "row";
	private static String OP_COL = "col";
	private static String OP_DISTR = "distr";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for missing value counting for all fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(MissingValueCounter.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(MissingValueCounter.CounterMapper.class);
        String operation = job.getConfiguration().get("mvc.counting.operation", "column");
        if (operation.equals(OP_COL) || operation.equals(OP_DISTR)) { 
            job.setCombinerClass(MissingValueCounter.CounterCombiner.class);
        }
    	job.setReducerClass(MissingValueCounter.CounterReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("mvc.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class CounterMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String[] items;
        private String fieldDelimRegex;
        private int[] idOrdinals;
        private String operation;
        private int count;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "mvc.field.delim.regex", "field.delim.regex", ",");
        	idOrdinals = Utility.intArrayFromString(config.get("mvc.id.field.ordinals"));
        	operation = config.get("mvc.counting.operation", "column");
        }    
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            if (operation.equals(OP_ROW)) {
            	//row wise
            	int beg = null != idOrdinals ? idOrdinals.length : 0;
            	count = BasicUtils.missingFieldCount(items, beg);
            	if (count > 0) {
                	//descending order of count
	       			outKey.initialize();
	       			outKey.add(Integer.MAX_VALUE - count);
	       			
	       			//record ID or whole record
	       			outVal.initialize();
	       			String rec = null != idOrdinals ? 
	       				BasicUtils.extractFields(items, idOrdinals, BasicUtils.DEF_FIELD_DELIM, false) :
	       				value.toString();
	       			outVal.add(rec);
	            	context.write(outKey, outVal);
            	}
            } else if (operation.equals(OP_COL)) {
            	//column wise
            	emiMissingColumn(context, null);
            } else if (operation.equals(OP_DISTR)) {
            	//row wise
            	int beg = null != idOrdinals ? idOrdinals.length : 0;
            	count = BasicUtils.missingFieldCount(items, beg);
            	if (count > 0) {
	       			outKey.initialize();
	       			outKey.add("row", count);
	    			
	       			outVal.initialize();
	    			outVal.add(1);
	            	context.write(outKey, outVal);
	            }
            	
            	//column wise
            	emiMissingColumn(context, "col");
            	
            } else {
            	throw new IllegalStateException("invalid operation");
            }
        }
        
        /**
         * @param context
         * @param keyPrefix
         * @throws IOException
         * @throws InterruptedException
         */
        private void emiMissingColumn(Context context, String keyPrefix) throws IOException, InterruptedException {
        	int i =  null != idOrdinals ? idOrdinals.length : 0;
        	for ( ; i < items.length; ++i) {
        		if (items[i].isEmpty()) {
        			//column ordinal
        			outKey.initialize();
        			if (null != keyPrefix) {
        				outKey.add(keyPrefix);
        			}
        			outKey.add(i);
        			
        			//count
        			outVal.initialize();
        			outVal.add(1);
                	context.write(outKey, outVal);
        		}
        	}
        	
        }
	}	

	/**
	 * @author pranab
	 *
	 */
	public static class CounterCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private int count;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	outVal.initialize();
        	count = 0;
    		for (Tuple val : values) {
    			count += val.getInt(0);
    		}
    		outVal.add(count);
        	context.write(key, outVal);
        }	
	}
	
	/**
	* @author pranab
  	*
  	*/
	public static class CounterReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
        private String operation;
        private int count;
        private int colMissingCountMin;
        private int rowMissingCountMin;
        private ValueCounters<Integer> colCounters = new ValueCounters<Integer>(false);
        private ValueCounters<String> distrRowCounters = new ValueCounters<String>(false);
        private ValueCounters<String> distrColCounters = new ValueCounters<String>(false);
        
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	operation = config.get("mvc.counting.operation", "column");
        	colMissingCountMin = config.getInt("mvc.col.missing.count.min", -1);
        	rowMissingCountMin = config.getInt("mvc.row.missing.count.min", -1);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
            if (operation.equals(OP_COL)) {
            	//column index and  missing value count
            	emitCounters(colCounters, colMissingCountMin, context);
            } else if (operation.equals(OP_DISTR)) {
            	//row wiae missing field count distribution
            	emitCounters(distrRowCounters, rowMissingCountMin, context);
            	
               	//column wise missing field count distribution
            	for(Integer key :  colCounters.getCounters().keySet()){
            		int count = colCounters.getCounters().get(key).getCount();
            		String colKey = "col" + BasicUtils.configDelim + count;
            		distrColCounters.add(colKey, 1);
            	}
            	 emitCounters(distrColCounters, colMissingCountMin, context);
            }
			super.cleanup(context);
		}		
		
		/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
            if (operation.equals(OP_ROW)) {
            	count = Integer.MAX_VALUE - key.getInt(0);
        		//only if the count is above threshold if specified
            	if (rowMissingCountMin == -1 || count > rowMissingCountMin) {
	        		for (Tuple val : values) {
	        			//record or record key followed by count
	            		outVal.set("" + val.getString(0) +  fieldDelim + count);
	            		context.write(NullWritable.get(), outVal);
	        		} 
            	}
            } else if (operation.equals(OP_COL)) {
        		getCount(values);
        		colCounters.add(key.getInt(0), count);
            } else if (operation.equals(OP_DISTR)) {
        		String obj = key.toString();
        		getCount(values);
        		String type = key.getString(0);
            	if (type.equals(OP_ROW)) {
            		distrRowCounters.add(obj, count);
            	} else {
            		colCounters.add(key.getInt(1), count);
            		//distrColCounters.add(obj, count);
            	}
            }
    	}	
    	
    	/**
    	 * @param values
    	 */
    	private void getCount(Iterable<Tuple> values) {
        	count = 0;
    		for (Tuple val : values) {
    			count += val.getInt(0);
    		}
    	}
    	
		/**
		 * @param colCounters
		 * @param minCount
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private <T> void emitCounters(ValueCounters<T> counters, int minCount, Context context) 
			throws IOException, InterruptedException {
        	List<ValueCounter<T>> sortedcounters = counters.getSorted();
        	for (ValueCounter<T> counter : sortedcounters) {
        		int count = counter.getCount();
        		if (minCount == -1  || count > minCount) {
        			outVal.set("" + counter.getObj() + fieldDelim +count);
        			context.write(NullWritable.get(), outVal);
        		} else {
        			break;
        		}
        	}
		}
    	
	}	
	
	/**
	 * @author pranab
	 *
	 */
	private static class MissingColumnCounter extends Pair<Integer, Integer>  implements Comparable {
		public MissingColumnCounter(Integer ordinal, Integer count) {
			super(ordinal, count);
		}
		
		@Override
		public int compareTo(Object other) {
			MissingColumnCounter that = (MissingColumnCounter)other;
			int thisCount = this.getRight();
			int thatCount = that.getRight();
			return thatCount - thisCount;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MissingValueCounter(), args);
		System.exit(exitCode);
	}
	
}
