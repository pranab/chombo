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
import java.util.Map;

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
import org.chombo.stats.CategoricalHistogramStat;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class CategoricalAttrClassDistr extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Categorical attribute class distribution";
        job.setJobName(jobName);
        
        job.setJarByClass(CategoricalAttrClassDistr.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(CategoricalAttrClassDistr.DistrMapper.class);
        job.setReducerClass(CategoricalAttrClassDistr.DistrReducer.class);
        job.setCombinerClass(CategoricalAttrClassDistr.DistrCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("cacd.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class DistrMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private String[] items;
        private GenericAttributeSchema schema;
        private int[] partIdOrdinals;
        private static final int ONE = 1;
        private int classAttrOrdinal;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	attributes = Utility.assertIntArrayConfigParam(config, "cacd.attr.list", Utility.configDelim, "missing attribute ordinals");
        	partIdOrdinals = Utility.intArrayFromString(config.get("cacd.id.field.ordinals"),  Utility.configDelim);
        	classAttrOrdinal = Utility.assertIntConfigParam(config, "cacd.class.attr.ordinal", "missing class attribute ordinal");
        	
        	//validate attributes
           	schema = Utility.getGenericAttributeSchema(config,  "cacd.schema.file.path");
            if (null != schema) {
           		if (!schema.areCategoricalAttributes(attributes)) {
        			throw new IllegalArgumentException("attributes must be categorical");
        		}
            }
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	for (int attr : attributes) {
            	outKey.initialize();
            	outVal.initialize();

            	//key
            	if (null != partIdOrdinals) {
            		outKey.addFromArray(items, partIdOrdinals);
            	}
            	
            	//attribute ordinal and value
            	outKey.add(attr, items[attr]);
            	
            	//class attribute value
            	outVal.add(items[classAttrOrdinal], ONE);
            	context.write(outKey, outVal);
        	}
        }
	}
	
	
	/**
	 * @author pranab
	 *
	 */
	public static class DistrCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private Map<String, Integer> classValueCounts = new HashMap<String, Integer>();
		private String classVal;
		private Integer classValCount;
		
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	classValueCounts.clear();
    		for (Tuple val : values) {
				classVal = val.getString(0);
				classValCount = val.getInt(1);
				Integer curAttrValCount = classValueCounts.get(classVal);
				if (null == curAttrValCount) {
					curAttrValCount = classValCount;
				} else {
					curAttrValCount += classValCount;
				}
				classValueCounts.put(classVal, classValCount);
	    		outVal.initialize();
	    		for (String thisClassValue :  classValueCounts.keySet()) {
	        		outVal.add(thisClassValue,  classValueCounts.get(thisClassValue));
	    		}
	        	context.write(key, outVal);       	
    		}	
        }
	}
	

	/**
	* @author pranab
  	*
  	*/
	public static class DistrReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		protected CategoricalHistogramStat histogram = new CategoricalHistogramStat();
		private String classVal;
		private Integer classValCount;
		private int outputPrecision;
       
        
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	outputPrecision = config.getInt("cacd.output.precision", 3);
        	histogram.withOutputPrecision(outputPrecision);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			histogram.intialize();
			for (Tuple val : values) {
				for (int i = 0; i < val.getSize(); i += 2) {
					classVal = val.getString(0);
					classValCount = val.getInt(1);
					histogram.add(classVal, classValCount);
				}
			}
			stBld.delete(0, stBld.length());
			key.setDelim(fieldDelim);
			stBld.append(key.toString()).append(fieldDelim).append(histogram.toString());
			outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
		}
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CategoricalAttrClassDistr(), args);
		System.exit(exitCode);
	}
	
}
