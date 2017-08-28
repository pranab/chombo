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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.chombo.util.Attribute;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts occurences of specified values for specified columns. Can be used for counting
 * missing values
 * @author pranab
 *
 */
public class ValueCounter  extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for specific  value counting for various fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(ValueCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(ValueCounter.CounterMapper.class);
        job.setReducerClass(ValueCounter.CounterReducer.class);
        job.setCombinerClass(ValueCounter.CounterCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("vlc.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class CounterMapper extends Mapper<LongWritable, Text,  Tuple, IntWritable> {
		private Tuple outKey = new Tuple();
		private IntWritable outVal = new IntWritable(1);
		private int[]  attributes;
        private String[] items;
        private String fieldDelimRegex;
        private GenericAttributeSchema schema;
        private Map<Integer, String[]> attrValues = new HashMap<Integer, String[]>();
        private Set<Integer> caseInsensitiveAttributeSet = new HashSet<Integer>();
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "vlc.schema.file.path");
            attributes = Utility.getAttributes("vlc..attr.list", configDelim,  schema, config,  Attribute.DATA_TYPE_CATEGORICAL, 
            		Attribute.DATA_TYPE_DATE, Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_STRING);        	

        	//attribute values
    		for (int ord : attributes ) {
    			String key = "vlc.values." + ord;
    			String values = config.get(key);
    			if (null != values) {
    				//specified values
    				attrValues.put(ord,  values.split(configDelim));
    			} else {
    				//missing values
    				String[] valueList = new String[1];
    				valueList[0] = "";
       				attrValues.put(ord,  values.split(configDelim));
       			}
    		}
    		
    		//case insentive attributes
    		int[] caseInsensitiveAttributes = Utility.intArrayFromString(config.get("vlc.case.insensitive.attr.list"),configDelim );
    		if (null !=caseInsensitiveAttributes) {
    			for (int attr :  caseInsensitiveAttributes) {
    				caseInsensitiveAttributeSet.add(attr);
    			}
    		}
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            boolean caseInsensitive = false;
            boolean matched = false;
            //all attributes
            for (int attr :  attrValues.keySet()) {
            	//all matching values
            	caseInsensitive = caseInsensitiveAttributeSet.contains(attr);
            	for (String attrValue :  attrValues.get(attr)) {
            		matched = caseInsensitive ? items[attr].equalsIgnoreCase(attrValue) : items[attr].equals(attrValue);
            		if (matched) {
            			outKey.initialize();
            			outKey.add(attr, attrValue);
            			context.write(outKey, outVal);
            		}
            	}
            }
        }
        
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class CounterCombiner extends Reducer<Tuple, IntWritable, Tuple, IntWritable> {
		private IntWritable outVal = new IntWritable();
		private int count;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context)
        		throws IOException, InterruptedException {
        	count = 0;
    		for (IntWritable val : values) {
    			count += val.get();
    		}
    		outVal.set(count);
        	context.write(key, outVal);
        }	
	}
	
	   /**
	    * @author pranab
	*
	*/
	public static class CounterReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
		protected Text outVal = new Text();
		protected String fieldDelim;
		private int count;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
 		}
	   	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
 	 	*/
		protected void reduce(Tuple key, Iterable<IntWritable> values, Context context)
     	throws IOException, InterruptedException {
			key.setDelim(fieldDelim);
			count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
 		
			outVal.set(key.toString() + fieldDelim + count );
			context.write(NullWritable.get(), outVal);
		}		
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ValueCounter(), args);
		System.exit(exitCode);
	}
	
}
