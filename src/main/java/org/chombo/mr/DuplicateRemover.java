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
import org.chombo.util.BasicUtils;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Removes duplicate records.Dedup can be based on whole record or set of specified key fields
 * @author pranab
 *
 */
public class DuplicateRemover extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "duplicate remover  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(DuplicateRemover.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DuplicateRemover.DuplicateMapper.class);
        job.setReducerClass(DuplicateRemover.DuplicateReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("dur.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class DuplicateMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private int[] keyAttributes;
        private String fieldDelimRegex;
        private String[] items;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	keyAttributes = Utility.intArrayFromString(config, "dur.key.field.ordinals", Utility.configDelim);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	keyOut.initialize();
        	valOut.initialize();
        	
        	if (null == keyAttributes) {
        		//whole record based dedup
        		keyOut.add(value.toString());
        		valOut.add(1);
        	} else {
        		//key fields based dedup
        		String keyRec = BasicUtils.extractFields(items, keyAttributes, fieldDelimRegex);
        		keyOut.add(keyRec);
        		valOut.add(value.toString());
        	}
           	context.write(keyOut, valOut);
        }        
    }

    
    /**
     * @author pranab
     *
     */
    public static class DuplicateReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private int count;
    	private static String OUT_ALL = "all";
    	private static String OUT_UNIQUE = "unique";
    	private static String OUT_DUP = "duplicate";
    	private boolean dupOnly;
    	private boolean uniqueOnly;
    	private int[] keyAttributes;
    	private Text valVout = new Text();
    	private String rec;
    	private boolean doEmit;
    	private boolean outputWholeRec;
    	
 
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	keyAttributes = Utility.intArrayFromString(config, "dur.key.field.ordinals", Utility.configDelim);
        	String outputMode = config.get("dur.output.mode", "all");
        	dupOnly = outputMode.equals(OUT_DUP);
        	uniqueOnly = outputMode.equals(OUT_UNIQUE);
        	outputWholeRec = config.getBoolean("dur.output.whole.rec", true);
        }
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
       		doEmit = true;
       		rec = null;
        	if (dupOnly || uniqueOnly) {
        		doEmit = false;
        		count = 0;
            	for(Tuple value : values) {
            		++count;
            		if (null != keyAttributes && null == rec) {
            			rec = value.getString(0);
            		}
            	}
            	if (uniqueOnly && count == 1 || dupOnly && count > 1){
            		doEmit = true;
            	}
        	} 
      	
        	if (doEmit) {
	        	if (null == keyAttributes) {
	        		//whole rec base dedup
	            	valVout.set(key.getString(0));
	        	} else {
	        		//key fields based dedup
	        		if (outputWholeRec) {
	        			valVout.set(rec);
	        		} else {
	        			valVout.set(key.getString(0));
	        		}
	        	}
	        	context.write(NullWritable.get(), valVout);
        	}
        }
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DuplicateRemover(), args);
        System.exit(exitCode);
    }
    
}
