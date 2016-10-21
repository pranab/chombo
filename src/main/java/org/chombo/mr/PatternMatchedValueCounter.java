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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Attribute;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts occurences of  values that match specified regex patterns for specified columns. Can be used 
 * for counting missing values.
 * @author pranab
 *
 */
public class PatternMatchedValueCounter extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for  counting values matching specified patterns for various fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(PatternMatchedValueCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(PatternMatchedValueCounter.CounterMapper.class);
        job.setReducerClass(ValueCounter.CounterReducer.class);
        job.setCombinerClass(ValueCounter.CounterCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("pmvc.num.reducer", -1);
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
        private Map<Integer, String[]> attrPatternTexts = new HashMap<Integer, String[]>();
        private Map<Integer, Pattern[]> attrPatterns = new HashMap<Integer, Pattern[]>();
        private Matcher matcher;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "pmvc.schema.file.path");
            attributes = Utility.getAttributes("pmvc..attr.list", configDelim,  schema, config,  Attribute.DATA_TYPE_CATEGORICAL, 
            		Attribute.DATA_TYPE_DATE, Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_STRING);        	
       	
        	//attribute value patterns
    		for (int ord : attributes ) {
    			String key = "pmvc.patterns." + ord;
    			String values = config.get(key);
    			if (null != values) {
    				//specified pattern tests
    				String[] pattrenTexts = values.split(configDelim);
    				Pattern[] patterns = new Pattern[pattrenTexts.length];
    				int i = 0;
    				for (String pattrenText :  pattrenTexts) {
    					patterns[i++] = Pattern.compile(pattrenText);
    				}
    				attrPatterns.put(ord,  patterns);
    				attrPatternTexts.put(ord, pattrenTexts);
    			} 
    		}
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            //all attributes
            for (int attr :  attrPatterns.keySet()) {
            	//all matching values
            	int i = 0;
            	String[] patternTexts = attrPatternTexts.get(attr);
            	for (Pattern pattern :  attrPatterns.get(attr)) {
            		matcher = pattern.matcher(items[attr]);
            		if (matcher.matches()) {
            			outKey.initialize();
            			outKey.add(attr, patternTexts[i]);
            			context.write(outKey, outVal);
            		}
            		++i;
            	}
            }
        }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PatternMatchedValueCounter(), args);
		System.exit(exitCode);
	}
	
}
