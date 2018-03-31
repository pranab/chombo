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
import org.chombo.util.Attribute;
import org.chombo.util.DuplicateRemover;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts unique values or cardinality for fields except for double and text type
 * @author pranab
 *
 */
public class UniqueValueCounter  extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for unique value counting for various fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(UniqueValueCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(UniqueValueCounter.CounterMapper.class);
        job.setReducerClass(UniqueValueCounter.CounterReducer.class);
        job.setCombinerClass(UniqueValueCounter.CounterCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("unc.num.reducer", -1);
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
        private GenericAttributeSchema schema;
        private boolean enforceSchema;
        private int[] partIdOrdinals;
        private int[] idOrdinals;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "unc.field.delim.regex", "field.delim.regex", ",");
        	enforceSchema = config.getBoolean("unc.enforce.schema", true);
        	
        	if (enforceSchema) {
        		schema = Utility.getGenericAttributeSchema(config,  "unc.schema.file.path");
        		attributes = Utility.getAttributes("unc.attr.list", configDelim,  schema, config,  Attribute.DATA_TYPE_CATEGORICAL, 
            		Attribute.DATA_TYPE_DATE, Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_STRING);  
        	} else {
        		attributes =  Utility.intArrayFromString(config.get("unc.attr.list"));
        	}
        	
        	partIdOrdinals = Utility.intArrayFromString(config.get("unc.part.id.field.ordinals"));
        	idOrdinals = Utility.intArrayFromString(config.get("unc.id.field.ordinals"));
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            if (null != attributes) {
            	//fixed number of columns
	        	for (int attr : attributes) {
	        		emit(attr, context);
	        	}
            } else {
            	//variable number of columns
            	int skipCount = null != partIdOrdinals ? partIdOrdinals.length : 0;
            	skipCount += null != idOrdinals ? idOrdinals.length : 0;
            	for (int i = skipCount; i < items.length; ++i) {
	        		emit(i, context);
            	}
            }
        }
        
        /**
         * @param attr
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void emit(int attr, Context context) throws IOException, InterruptedException {
    		outKey.initialize();
    		outVal.initialize();
    		
        	if (null != partIdOrdinals) {
        		outKey.addFromArray(items, partIdOrdinals);
        	}
    		outKey.add(attr);
    		
    		outVal.add(items[attr]);
        	context.write(outKey, outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class CounterCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private DuplicateRemover<String> dupRemover = new DuplicateRemover<String>(); 
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	dupRemover.initialize();
        	outVal.initialize();
    		for (Tuple val : values) {
    			for (int i = 0; i < val.getSize(); ++i) {
    				dupRemover.add(val.getString(i));
    			}
    		}
    		
    		for (String uniqueValue : dupRemover.getData()) {
    			outVal.add(uniqueValue);
    		}
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
		private DuplicateRemover<String> dupRemover = new DuplicateRemover<String>(); 
		private boolean outputCount;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	outputCount = config.getBoolean("unc.output.count", true);
		}
	   	
		/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
    		dupRemover.initialize();
    		for (Tuple val : values) {
    			for (int i = 0; i < val.getSize(); ++i) {
    				dupRemover.add(val.getString(i));
    			}
    		}
    		
       		stBld.delete(0, stBld.length());
    		if (outputCount) {
    			//count
	       		stBld.append(key.toString()).append(fieldDelim).append(dupRemover.getSize()).append(fieldDelim);
    		} else {
    			//actual values
	       		stBld.append(key.toString()).append(fieldDelim);
	    		for (String uniqueValue : dupRemover.getData()) {
	    			stBld.append(uniqueValue).append(fieldDelim);
	    		}    	
    		}
    		outVal.set(stBld.substring(0, stBld.length() -1));
			context.write(NullWritable.get(), outVal);
    	}		
 	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UniqueValueCounter(), args);
		System.exit(exitCode);
	}
 
}
