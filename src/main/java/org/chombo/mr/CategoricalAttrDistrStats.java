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
import org.chombo.util.CategoricalHistogramStat;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class CategoricalAttrDistrStats  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Distribution based stats for categorical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(CategoricalAttrDistrStats.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(CategoricalAttrDistrStats.StatsMapper.class);
        job.setReducerClass(CategoricalAttrDistrStats.StatsReducer.class);
        job.setCombinerClass(CategoricalAttrDistrStats.StatsCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("cads.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class StatsMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private int conditionedAttr;
        private String[] items;
        private GenericAttributeSchema schema;
        private static final int ONE = 1;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	attributes = Utility.intArrayFromString(config.get("cads.attr.list"),fieldDelimRegex );
        	conditionedAttr = config.getInt("cads.conditioned.attr",-1);
        	
        	//validate attributes
           	schema = Utility.getGenericAttributeSchema(config,  "schema.file.path");
            if (null != schema) {
           		if (!schema.areCategoricalAttributes(attributes)) {
        			throw new IllegalArgumentException("attributes must be categorical");
        		}
            }
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	for (int attr : attributes) {
            	outKey.initialize();
            	outVal.initialize();
            	outKey.add(attr, "0");
            	outVal.add(items[attr], ONE);
            	context.write(outKey, outVal);

            	//conditioned on another attribute
            	if (conditionedAttr >= 0) {
                	outKey.initialize();
                	outVal.initialize();
                	outKey.add(attr, items[conditionedAttr]);
                	outVal.add(items[attr], ONE);
                	context.write(outKey, outVal);
            	}
        	}
        }
 	}

	/**
	 * @author pranab
	 *
	 */
	public static class StatsCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private Map<String, Integer> attrValueCounts = new HashMap<String, Integer>();
		
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		attrValueCounts.clear();
    		for (Tuple val : values) {
    			for (int i = 0; i < val.getSize(); i += 2) {
    				String attrVal = val.getString(i);
    				Integer attrValCount = val.getInt(i + 1);
    				Integer curAttrValCount = attrValueCounts.get(attrVal);
    				if (null == curAttrValCount) {
    					curAttrValCount = attrValCount;
    				} else {
    					curAttrValCount += attrValCount;
    				}
    				 attrValueCounts.put(attrVal, curAttrValCount);
    			}
    		}
    		outVal.initialize();
    		for (String thisAttrValue :  attrValueCounts.keySet()) {
        		outVal.add(thisAttrValue,  attrValueCounts.get(thisAttrValue));
    		}
        	context.write(key, outVal);       	
        }		
	}	
	
	/**
	* @author pranab
  	*
  	*/
	public static class StatsReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		protected CategoricalHistogramStat histogram = new CategoricalHistogramStat();
        private int conditionedAttr;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	conditionedAttr = config.getInt("cads.conditioned.attr",-1);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			histogram.intialize();
			for (Tuple val : values) {
				for (int i = 0; i < val.getSize(); i += 2) {
					String attrVal = val.getString(i);
					Integer attrValCount = val.getInt(i + 1);
					histogram.add(attrVal, attrValCount);
				}
			}
			emitOutput( key,  context);
		}
 	
		/**
		 * @param key
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected  void emitOutput(Tuple key,  Context context) throws IOException, InterruptedException {
			stBld.delete(0, stBld.length());
			stBld.append(key.getInt(0)).append(fieldDelim);
			if (conditionedAttr != -1) {
				stBld.append(key.getString(1)).append(fieldDelim);
			}
			Map<String, Double> distr = histogram.getDistribution();
			for (String  attrValue : distr.keySet() ) {
				stBld.append(attrValue).append(fieldDelim).append(distr.get(attrValue)).append(fieldDelim);
			}
			stBld.append(histogram.getEntropy()) ;
			stBld.append(fieldDelim).append(histogram.getMode()) ;
			outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CategoricalAttrDistrStats(), args);
		System.exit(exitCode);
	}

}
