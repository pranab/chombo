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
 * From a record containing multiple keys and values laid out in different formats, generates 
 * key, value output with one key and value(s) per record
 */
public class KeyValueFormatter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Key value formatter MR";
        job.setJobName(jobName);
        
        job.setJarByClass(KeyValueFormatter.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(KeyValueFormatter.FormatterMapper.class);
        job.setReducerClass(KeyValueFormatter.FormatterReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
  
        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("kvf.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 */
	public static class FormatterMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private boolean alternateKeyValue;
        private boolean  keyFirst;
        private int keyLength;
        private int valueLength;
        private int keyValueOffset;
        private int[] repeatingKeyFields;
        private int[] repeatingValueFields;
        private int keyValueLength;
        private int keyOffset;
        private int valueOffset;
        
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "kvf.field.delim.regex", "field.delim.regex", ",");
        	alternateKeyValue = config.getBoolean("kvf.alternate.key.value", true);
        	keyFirst = config.getBoolean("kvf.key.first", true);
        	keyLength = config.getInt("kvf.key.length", 1);
        	valueLength = config.getInt("kvf.value.length", 1);
        	keyValueOffset = Utility.assertIntConfigParam(config, "kvf.key.value.offset", "missing key value offset");
        	repeatingKeyFields = Utility.intArrayFromString(config, "kvf.repeating.key.fields");
        	repeatingValueFields = Utility.intArrayFromString(config, "kvf.repeating.value.fields");
        	keyValueLength = keyLength + valueLength;
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            if (alternateKeyValue) {
            	//alternate ke and value
            	if (keyFirst) {
            		keyOffset = keyValueOffset;
            		valueOffset = keyValueOffset + keyLength;
            	} else {
            		keyOffset = keyValueOffset + valueLength;
            		valueOffset = keyValueOffset;
            	}
            	
            	//all key value pairs
            	while (keyOffset < items.length) {
            		setKeyValue();
        			context.write(outKey, outVal);
        			keyOffset += keyValueLength;
        			valueOffset += keyValueLength;
            	}
            } else {
            	//keys and values together
            	int numKeyValues = (items.length - keyValueOffset) / keyValueLength;
            	if (keyFirst) {
            		keyOffset = keyValueOffset;
            		valueOffset = keyValueOffset + numKeyValues * keyLength;
            	} else {
            		keyOffset = keyValueOffset + numKeyValues * valueLength;
            		valueOffset = keyValueOffset;
            	}
            	
            	//all key value pairs
            	for (int i = 0; i < numKeyValues; ++i) {
            		setKeyValue();
        			context.write(outKey, outVal);
        			keyOffset += keyLength;
        			valueOffset += valueLength;
            	}
            }
        }
        
        /**
         * 
         */
        private void setKeyValue() {
    		outKey.initialize();
    		Utility.appendStringTuple(items, keyOffset, keyOffset + keyLength,  outKey);
    		if (null != repeatingKeyFields) {
    			Utility.appendStringTuple(items, repeatingKeyFields, outKey);
    		}
    		
    		outVal.initialize();
    		Utility.appendStringTuple(items, valueOffset, valueOffset + valueLength,  outVal);
    		if (null != repeatingValueFields) {
    			Utility.appendStringTuple(items, repeatingValueFields, outVal);
    		}
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class FormatterReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private StringBuilder stBld = new StringBuilder();
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            fieldDelim = config.get("field.delim.out", ",");
        }	
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString());
    		
        	for (Tuple value : values){
        		value.setDelim(fieldDelim);
        		stBld.append(fieldDelim).append(value.toString());
        	}
			outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    }	
    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KeyValueFormatter(), args);
        System.exit(exitCode);
	}
    
}
