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
import org.chombo.util.BasicUtils;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Categorical attribute distance by Value Difference Metric. Uses output of CategoricalAttrClassDistr
 * @author pranab
 *
 */
public class CatAttrDistByValueDiffMetric extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Categorical attribute dist by Value Difference Metric";
        job.setJobName(jobName);
        
        job.setJarByClass(CatAttrDistByValueDiffMetric.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(CatAttrDistByValueDiffMetric.DistMapper.class);
        job.setReducerClass(CatAttrDistByValueDiffMetric.DistReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("cvdm.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class DistMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int partIdLen;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	partIdLen = config.getInt("cvdm.id.field.len", 0);
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	outKey.initialize();
        	outVal.initialize();
        	
        	//partition id and attribute ordinal
        	int offset = partIdLen + 1;
        	outKey.addFromArray(items, 0, offset);
        	
        	//attribute value
        	outVal.add(items[offset++]);
        	
        	//class value distribution
        	while (offset < items.length) {
        		outVal.add(items[offset++]);
        		outVal.add(Double.parseDouble(items[offset++]));
        	}
        	context.write(outKey, outVal);
        }        
	}
	
	
	/**
	* @author pranab
  	*
  	*/
	public static class DistReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld = new StringBuilder();
		protected String fieldDelim;
		protected Map<String, Map<String, Double>> attrClassValDistr  = new HashMap<String, Map<String, Double>>();
		private int outputPrecision;
		private String[] clValList;
		private List<String> attrValList = new ArrayList<String>();
		private int distExp;
       
        
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	outputPrecision = config.getInt("cvdm.output.precision", 3);
        	clValList = Utility.assertStringArrayConfigParam(config, "cvdm.class.values", Utility.configDelim, 
        			"missing class attribute value list");
        	distExp = config.getInt("cvdm.dist.exp", 1);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			attrClassValDistr.clear();
			attrValList.clear();
			for (Tuple val : values) {
				int i = 0;
				String attrVal = val.getString(i++);
				Map<String, Double> classValDistr = new HashMap<String, Double>();
				attrClassValDistr.put(attrVal, classValDistr);
				attrValList.add(attrVal);
				
				while(i < val.getSize()) {
					String classVal = val.getString(i++);
					Double classValPr = val.getDouble(i++);
					classValDistr.put(classVal, classValPr);
				}
			}
			
			//value diff metric
			for (int i = 0; i < attrValList.size(); ++i) {
				String firstVal = attrValList.get(i);
				for (int j = i+1; j < attrValList.size(); ++j) {
					String secondVal = attrValList.get(j);
					
					//dist between two values
					double dist = 0;
					for (String clVal : clValList) {
						Double firstPr = attrClassValDistr.get(firstVal).get(clVal);
						firstPr = firstPr == null ? 0 : firstPr;
						
						Double secondPr = attrClassValDistr.get(secondVal).get(clVal);
						secondPr = secondPr == null ? 0 : secondPr;
						
						double diff = Math.abs(firstPr - secondPr);
						dist += Math.pow(diff, distExp);
					}
					
					//emit
					stBld.delete(0, stBld.length());
					key.setDelim(fieldDelim);
					stBld.append(key.toString()).append(fieldDelim).append(firstVal).
						append(fieldDelim).append(secondVal).append(BasicUtils.formatDouble(dist, outputPrecision));
					outVal.set(stBld.toString());
					context.write(NullWritable.get(), outVal);
				}
			}
		}	
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CatAttrDistByValueDiffMetric(), args);
		System.exit(exitCode);
	}
	
}
