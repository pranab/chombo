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
import org.apache.hadoop.mapreduce.Reducer.Context;
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
        private int[] partIdOrdinals;
        private static final int ONE = 1;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	attributes = Utility.assertIntArrayConfigParam(config, "cads.attr.list", Utility.configDelim, "missing attribute ordinals");
        	conditionedAttr = config.getInt("cads.conditioned.attr",-1);
        	partIdOrdinals = Utility.intArrayFromString(config.get("cads.id.field.ordinals"),  Utility.configDelim);
        	
        	//validate attributes
           	schema = Utility.getGenericAttributeSchema(config,  "cads.schema.file.path");
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
                String condAttrVal = conditionedAttr >= 0 ?  items[conditionedAttr] : "$";

            	if (null != partIdOrdinals) {
            		outKey.addFromArray(items, partIdOrdinals);
            	}
            	outKey.add(attr, condAttrVal);
            	
            	outVal.add(items[attr], ONE);
            	context.write(outKey, outVal);
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
		private int[]  attributes;
        private int conditionedAttr;
        private int[] partIdOrdinals;
        private boolean outputGlobalStats;
        private boolean shouldOutputGlobalStats;
        private Map<Integer, CategoricalHistogramStat> globalHistogram;
        private CategoricalHistogramStat attrHistogram;
        
        
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	attributes = Utility.assertIntArrayConfigParam(config, "cads.attr.list", Utility.configDelim, "missing attribute ordinals");
        	conditionedAttr = config.getInt("cads.conditioned.attr",-1);
        	partIdOrdinals = Utility.intArrayFromString(config.get("cads.id.field.ordinals"),  Utility.configDelim);
        	
        	outputGlobalStats = config.getBoolean("cads.output.global.stats", false);
        	shouldOutputGlobalStats = null != partIdOrdinals && outputGlobalStats;
        	if (shouldOutputGlobalStats ) {
        		globalHistogram = new HashMap<Integer, CategoricalHistogramStat>();
        		for (int attr :  attributes) {
        			globalHistogram.put(attr,  new CategoricalHistogramStat());
        		}
        	}
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (shouldOutputGlobalStats) {
        		for (int attr :  attributes) {
        			stBld.delete(0, stBld.length());
        			stBld.append("*").append(fieldDelim);
    				attrHistogram = globalHistogram.get(attr);
    				hstogramToString(attrHistogram, stBld);
    				outVal.set(stBld.toString());
    				context.write(NullWritable.get(), outVal);
        		}				
			}
		}		
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			histogram.intialize();
			if (shouldOutputGlobalStats ) {
				int attr = key.getInt(partIdOrdinals.length);
				attrHistogram = globalHistogram.get(attr);
			}
			for (Tuple val : values) {
				for (int i = 0; i < val.getSize(); i += 2) {
					String attrVal = val.getString(i);
					Integer attrValCount = val.getInt(i + 1);
					histogram.add(attrVal, attrValCount);
					
					//global stats
					if (shouldOutputGlobalStats ) {
						attrHistogram.add(attrVal, attrValCount);
					}
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
			int i = 0;
			
			//partition id
			if (null != partIdOrdinals) {
				stBld.append(key.toString(i, partIdOrdinals.length));
				i += partIdOrdinals.length;
			}
			
			//attr ordinal
			stBld.append(key.getInt(i)).append(fieldDelim);
			++i;
			
			//conditional attr
			if (conditionedAttr != -1) {
				stBld.append(key.getString(i)).append(fieldDelim);
			}
			
			hstogramToString(histogram, stBld);
			outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
		}
		
		/**
		 * @param histogram
		 */
		private void hstogramToString(CategoricalHistogramStat histogram, StringBuilder stBld) {
			Map<String, Double> distr = histogram.getDistribution();
			for (String  attrValue : distr.keySet() ) {
				stBld.append(attrValue).append(fieldDelim).append(distr.get(attrValue)).append(fieldDelim);
			}
			stBld.append(histogram.getEntropy()).append(fieldDelim) ;
			stBld.append(histogram.getGiniIndex()) ;
			stBld.append(fieldDelim).append(histogram.getMode()) ;
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
