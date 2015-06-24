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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.transformer.AttributeTransformer;
import org.chombo.util.Utility;

/**
 * Transforms attributes based on plugged in transformers for different attributes.
 * This class and the nested mapper calss need to be extended to do any useful work
 * @author pranab
 *
 */
public class Transformer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Transformer  MR";
        job.setJobName(jobName);
        job.setJarByClass(Transformer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(Transformer.TransformerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @param jobName
	 * @param jobClass
	 * @param mapClass
	 * @param args
	 * @return
	 * @throws Exception
	 */
	protected  int start(String jobName, Class<?> jobClass, Class<? extends Mapper> mapClass, String[] args)  
		throws Exception {
        Job job = new Job(getConf());
        job.setJobName(jobName);
        job.setJarByClass(jobClass);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(mapClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
      
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * Mapper for  attribute transformation
	 * @author pranab
	 *
	 */
	public static class TransformerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
		private Map<Integer, List<AttributeTransformer>> transformers = new HashMap<Integer, List<AttributeTransformer>>();
		private AttributeTransformer transformer;
		private String[] transformedValues;
        private String[] items;
        private String[] singleTransformedValue = new String[1];
        private List<AttributeTransformer>  transformerList;
        private String source;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = context.getConfiguration().get("field.delim", ",");
       }
        
        /**
         * @param fieldOrd
         * @param transformer
         */
        protected void registerTransformers(int fieldOrd, AttributeTransformer...  transformer) {
        	List<AttributeTransformer> transList = transformers.get(fieldOrd);
        	if (null == transList) {
        		transList = new ArrayList<AttributeTransformer>();
        		transformers.put(fieldOrd, transList);
        	}
        	
        	//add all
        	for (AttributeTransformer trans :  transformer) {
        		transList.add(trans);
        	}
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            stBld.delete(0, stBld.length());
            for (int i = 0; i < items.length; ++i) {
            	//either transform or pass through
            	transformerList = transformers.get(i);
            	int t = 0;
            	source = items[i];
            	
            	//all transformers
            	for (AttributeTransformer trans :  transformerList) {
	        		if (null !=trans) {
	        			transformedValues = trans.tranform(source);
	        			if (transformerList.size() > 1 && t <  transformerList.size() -1 && transformedValues.length > 1 ) {
	        				//only last transformer is allowed to emit multiple values
	        				throw new  IllegalStateException("for cascaded transformeronly last transformer is allowed to emit multiple values");
	        			}
	        		} else {
	        			singleTransformedValue[0] = source;
	        			transformedValues =  singleTransformedValue;
	        		}
	        		
	        		source = transformedValues[0];
	        		++t;
	            }
            	
            	
            	
        		//add to output
        		if (null != transformedValues) {
        			for (String transformedValue :  transformedValues) {
        				stBld.append(transformedValue).append(fieldDelimOut);
        			}
        		}
        		
            }
            outVal.set(stBld.substring(0, stBld.length() -1));
			context.write(NullWritable.get(), outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class NullTransformer implements AttributeTransformer {
		@Override
		public String[] tranform(String value) {
			String[] transformed = new String[1];
			transformed[0] = null;
			return transformed;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Transformer(), args);
        System.exit(exitCode);
	}
	
}
