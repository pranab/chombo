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
import java.util.HashSet;
import java.util.List;
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
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Analyzes functional dependency between attribute pairs. For example, state is a function of zip code
 * i.e. given a zip code there is only one corresponding state
 * @author pranab
 *
 */
public class FunctionalDependencyAnalyzer  extends Configured implements Tool {
	private static String configDelim = ",";
	private static String configSubFieldDelim = ":";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for analyzing functional depencency between  fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(FunctionalDependencyAnalyzer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(FunctionalDependencyAnalyzer.AnalyzerMapper.class);
        job.setReducerClass(FunctionalDependencyAnalyzer.AnalyzerReducer.class);
        job.setCombinerClass(FunctionalDependencyAnalyzer.AnalyzerCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("fda.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class AnalyzerMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String[] items;
        private String fieldDelimRegex;
        private List<Pair<Integer, Integer>> attrPairs;
        private GenericAttributeSchema schema;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "fda.schema.file.path");
            String attrPairStr = Utility.assertStringConfigParam(config, "fda.attr.pairs", "missing list of attribute paire");
            attrPairs = Utility.getIntPairList(attrPairStr, configDelim, configSubFieldDelim);
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            //all attribute pairs
        	for (Pair<Integer, Integer> attrPair : attrPairs) {
        		outKey.initialize();
        		outVal.initialize();
        		
        		int argAttr = attrPair.getLeft();
        		outKey.add(argAttr, items[argAttr]);
        		outVal.add(items[attrPair.getRight()]);
            	context.write(outKey, outVal);
        	}
        }
	}

	/**
	 * @author pranab
	 *
	 */
	public static class AnalyzerCombiner extends Reducer<IntWritable, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private Set<String> uniqueValues = new HashSet<String>();
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	uniqueValues.clear();
        	outVal.initialize();
    		for (Tuple val : values) {
    			for (int i =0; i < val.getSize(); ++i) {
    				uniqueValues.add(val.getString(i));
    			}
    		}
    		
    		//tuple with unique values
    		for (String uniqueValue : uniqueValues) {
    			outVal.add(uniqueValue);
    		}
        	context.write(key, outVal);
        }	
	}

	   /**
	    * @author pranab
	    *
	    */
	public static class AnalyzerReducer extends Reducer<IntWritable, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private Set<String> uniqueValues = new HashSet<String>();

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
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			uniqueValues.clear();
			for (Tuple val : values) {
				for (int i =0; i < val.getSize(); ++i) {
					uniqueValues.add(val.getString(i));
				}
			}
 		
    		stBld.delete(0, stBld.length());
    		key.setDelim(fieldDelim);
	       	stBld.append(key.toString()).append(fieldDelim);
	    	for (String uniqueValue : uniqueValues) {
	    		stBld.append(uniqueValue).append(fieldDelim);
	    	}    	
 
	    	outVal.set(stBld.substring(0, stBld.length() -1));
			context.write(NullWritable.get(), outVal);
		}		
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FunctionalDependencyAnalyzer(), args);
		System.exit(exitCode);
	}
	
	
}
