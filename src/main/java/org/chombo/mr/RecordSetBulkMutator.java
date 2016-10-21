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
 * Perform bulk insert, update and delete on a set of records. Incremental data containing
 * inserted, updated and deleted records are merged into base record set.
 * @author pranab
 *
 */
public class RecordSetBulkMutator  extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "record set mutator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(RecordSetBulkMutator.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RecordSetBulkMutator.BulkMutatorMapper.class);
        job.setReducerClass(RecordSetBulkMutator.BulkMutatorReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("rsbm.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    /**
     * @author pranab
     *
     */
    public static class BulkMutatorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelimRegex;
    	private String[] items;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isDeletedDataFileSplit;
    	private int[] idFieldOrdinals;
    	private int opCodeFieldOrdinal;
    	private String deleteOpCode;
    	private int temporalOrderingFieldFieldOrdinal;
    	private String opCode;
    	private boolean isTemporalOrderingFieldNumeric;
    	private long splitSequence;
    	private  String record;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
    		opCodeFieldOrdinal = config.getInt("rsbm.op.code.field.ordinal", -1);
    		String spliFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
    		deleteOpCode = config.get("rsbm.deleted.op.code", "D");
  			if ( opCodeFieldOrdinal <  0 ) {
            	String deletedRecFilePrefix = config.get("rsbm.deleted.record.file.prefix");
            	if (null == deletedRecFilePrefix) {
        			throw new IllegalArgumentException(
        					"deleted data should either be in files with with configured prefix in file name or op code field ordinal value should be provided ");

            	} else {
            		isDeletedDataFileSplit = spliFileName.startsWith(deletedRecFilePrefix);
            	}
    		}

  			temporalOrderingFieldFieldOrdinal = config.getInt("rsbm.temporal.ordering.field.ordinal", -1);
        	isTemporalOrderingFieldNumeric = config.getBoolean("rsbm.temporal.ordering.field.numeric", true);
        	
        	if (temporalOrderingFieldFieldOrdinal == -1) {
        		//get temporal sequence from file name
        		String[] items = spliFileName.split("_");
        		splitSequence = Long.parseLong(items[items.length-1]);
        	}
        	
        	idFieldOrdinals = Utility.intArrayFromString(config.get("rsbm.id.field.ordinals"));
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
  			
  			for (int ord : idFieldOrdinals ) {
  				keyOut.append(items[ord]);
  			}
  			if (temporalOrderingFieldFieldOrdinal >= 0) {
  				//temporal ordering field in record
  				if (isTemporalOrderingFieldNumeric) {
  					keyOut.append(Long.parseLong(items[temporalOrderingFieldFieldOrdinal]));
  				} else {
  					keyOut.append(items[temporalOrderingFieldFieldOrdinal]);
  				}
  			} else {
  				//temporal ordering based sequence encoded in file name
  				keyOut.append(splitSequence);
  			}
  			
  			
  			if ( opCodeFieldOrdinal >= 0 ) {
  				//identify deletes with op code
  	  			record = Utility.join(items, 0, opCodeFieldOrdinal) + fieldDelimRegex + Utility.join(items,  opCodeFieldOrdinal + 1, items.length);
  	  			valOut.add(record);
    			opCode = items[opCodeFieldOrdinal];
    			if (opCode.equals(deleteOpCode)) {
    				valOut.append(deleteOpCode);
    			} 
    		} else {
    			//identify deletes from split file name
      			valOut.add(value.toString());
    			if (isDeletedDataFileSplit) {
    				valOut.append(deleteOpCode);
    			}
    		}
           	
           	context.write(keyOut, valOut);
        }
    
    }
    
    /**
     * @author pranab
     *
     */
    public static class BulkMutatorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private String record;
    	private String opCode;
    	private int count;
 
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        }
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	opCode = null;
        	count = 0;
        	for(Tuple value : values) {
        		record = value.getString(0);
        		if (value.getSize() ==2) {
        			opCode = value.getString(1);
        		} else {
        			opCode = null;
        		}
        		++count;
        	}
        	if (null == opCode) {
        		//emit the last record only if it's not a delete
        		valOut.set(record);
        		context.write(NullWritable.get(), valOut);
        		if (count == 1) {
        			context.getCounter("Mutation type", "Insert").increment(1);
        		} else {
        			context.getCounter("Mutation type", "Update").increment(1);
        		}
        	} else {
    			context.getCounter("Mutation type", "Delete").increment(1);
        	}
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RecordSetBulkMutator(), args);
        System.exit(exitCode);
    }
    
}
