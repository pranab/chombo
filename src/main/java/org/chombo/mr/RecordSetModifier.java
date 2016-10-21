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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Modifies a record set by excluding certain records or by including certain records.
 * The record identifier field values are provided through a second set of files
 * @author pranab
 *
 */
public class RecordSetModifier  extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "record set modifier  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(RecordSetModifier.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RecordSetModifier.ModifierMapper.class);
        job.setReducerClass(RecordSetModifier.ModifierReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("rsm.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    /**
     * @author pranab
     *
     */
    public static class ModifierMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelimRegex;
    	private String[] items;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isRecIdFileSplit;
    	private int idFieldOrdinal;
    	private static final int FIELD_VALUE_ORDINAL = 0;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String recordIdentifierFilePrefix = config.get("rsm.record.identifier.file.prefix", "recid");
        	isRecIdFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(recordIdentifierFilePrefix);
        	idFieldOrdinal = config.getInt("rsm.id.field.ordinal", -1);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	items = value.toString().split(fieldDelimRegex, -1);
           	keyOut.initialize();
  			valOut.initialize();
           	
           	if (isRecIdFileSplit) {
           		//record identifier
           		keyOut.add(items[FIELD_VALUE_ORDINAL],0);
           		valOut.add(0);
           	} else {
           		//actual records
           		keyOut.add(items[idFieldOrdinal],1);
           		valOut.add(1, value.toString());
           	}
           	context.write(keyOut, valOut);
        }
    
    }
    
    /**
     * @author pranab
     *
     */
    public static class ModifierReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private boolean  toBeExcuded;
    	private boolean toBeFiletred;
 
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
           	toBeExcuded = config.getBoolean("rsm.exclude.rec", true);
        }
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	toBeFiletred = false;
        	for(Tuple value : values) {
        		int type = value.getInt(0);
        		if (0 == type) {
        			toBeFiletred = true;
        		}  else {
        			if (toBeFiletred && !toBeExcuded || !toBeFiletred && toBeExcuded) {
    					valOut.set(value.getString(1));
    					context.write(NullWritable.get(), valOut);
        			}
        		}
        	}
        }
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RecordSetModifier(), args);
        System.exit(exitCode);
    }
    
}
