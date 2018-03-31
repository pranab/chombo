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
import org.chombo.util.Attribute;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Determines uniqueness of key. If multiple records found with same key, such records are output
 * along with ocurence count of corresponding values
 * @author pranab
 *
 */
public class UniqueKeyAnalyzer extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for unique composite key analysis ";
        job.setJobName(jobName);
        
        job.setJarByClass(UniqueKeyAnalyzer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(UniqueKeyAnalyzer.AnalyzerMapper.class);
        job.setReducerClass(UniqueKeyAnalyzer.AnalyzerReducer.class);
        job.setCombinerClass(ValueCounter.CounterCombiner.class);
       
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("uka.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class AnalyzerMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {
		private Tuple outKey = new Tuple();
		private IntWritable outVal = new IntWritable(1);
		private int[]  keyOrdinals;
        private String[] items;
        private String fieldDelimRegex;
        private GenericAttributeSchema schema;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "uka.schema.file.path");
        	keyOrdinals = Utility.getAttributes("uka.comp.key.ordinals", configDelim,  schema, config,  Attribute.DATA_TYPE_CATEGORICAL, 
            		Attribute.DATA_TYPE_DATE, Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_STRING);        	
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            Utility.intializeTuple(items, keyOrdinals, schema, outKey);
           	context.write(outKey, outVal);
        }
	}

	   /**
	    * @author pranab
	*
	*/
	public static class AnalyzerReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
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
		
			if (count > 1) {
				outVal.set(key.toString() + fieldDelim + count );
				context.write(NullWritable.get(), outVal);
			}
		}		
	}	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UniqueKeyAnalyzer(), args);
		System.exit(exitCode);
	}
	
}
