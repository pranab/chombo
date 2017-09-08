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
import java.io.InputStream;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.stats.NumericalAttrStatsManager;
import org.chombo.stats.StatsParameters;
import org.chombo.util.Attribute;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.ProcessorAttributeSchema;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Normalized data using stats output from another map reduce
 * @author pranab
 *
 */
public class NumericalAttrNormalizer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Numerical attribute normalizer  MR";
        job.setJobName(jobName);
        job.setJarByClass(NumericalAttrNormalizer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(NumericalAttrNormalizer.NormalizerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * Mapper for  attribute transformation
	 * @author pranab
	 *
	 */
	public static class NormalizerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
		private String itemValue;
        private String[] items;
        private GenericAttributeSchema schema;
        private int scale;
        private Map<Integer, StatsParameters> attrStats = new HashMap<Integer, StatsParameters>();
        private int intFieldValue;
        private double dblFieldValue;
        private double normFieldValue;
        private String decFormat;
        private float outlierTruncationLevel;
        private Map<Integer, String> normalizers = new HashMap<Integer, String>();
        private StatsParameters stats;
        private Attribute attr;
        private ProcessorAttributeSchema cleanserSchema;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	
        	//schema
        	InputStream is = Utility.getFileStream(config, "nan.schema.file.path");
        	ObjectMapper mapper = new ObjectMapper();
        	schema = mapper.readValue(is, GenericAttributeSchema.class);
            
            //stats data
            String statsFilePath = config.get("nan.stats.file.path");
            if (null == statsFilePath) {
            	throw new IllegalArgumentException("stats file path missing");
            }
            NumericalAttrStatsManager statsManager = new NumericalAttrStatsManager(config, statsFilePath, ",");
            
            //mean and std dev
            for (int i = 0; i < schema.getAttributeCount(); ++i) {
            	Attribute attr = schema.findAttributeByOrdinal(i);
            	if (attr.isInteger() || attr.isDouble()) {
            		attrStats.put(i, statsManager.getStatsParameters(i));
            	}
            }
            
            //scaling data
            scale = config.getInt("nan.attr.scale", -1);
            
            //decimal formatting
            int decPrecision = config.getInt("nan.dec.precision", 3); 
            decFormat = "%." + decPrecision + "f";
            
            //outlier truncation level
            outlierTruncationLevel = config.getFloat("nan.outlier.truncation.level", (float)-1.0);

        	//data cleanser schema
            String cleanserSchemPath = config.get("nan.cleanser.schema.file.path");
            if (null != cleanserSchemPath) {
            	is = Utility.getFileStream(config, "nan.cleanser.schema.file.path");
            	mapper = new ObjectMapper();
            	cleanserSchema = mapper.readValue(is, ProcessorAttributeSchema.class);
            	for (int i : cleanserSchema.getAttributeOrdinals()) {
            		ProcessorAttribute attr = cleanserSchema.findAttributeByOrdinal(i);
            		if (attr.isInteger() || attr.isDouble()) {
            			normalizers.put(i, attr.getNormalizerStrategy());
            		}
            	}
            	
            } else {
            	String[] items = config.get("nan.attr.normalizer.list").split(",");
            	for (String item : items) {
            		String[] parts = item.split(":");
            		normalizers.put(Integer.parseInt(parts[0]), parts[1]);
            	}
            }
        }
        

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            stBld.delete(0, stBld.length());
            boolean toInclude = true;
            
            for (int i = 0; i < items.length; ++i) {
            	toInclude = true;
            	itemValue = items[i];
            	attr = schema.findAttributeByOrdinal(i);
            	stats = attrStats.get(i);
            	
            	if (attr.isInteger()) {
            		intFieldValue = Integer.parseInt(itemValue);
            		if (normalizers.get(i).equals("zScore")) {
	            		normFieldValue = (intFieldValue - stats.getMean()) / stats.getStdDev();
	        			if (isOutlier()) {
	        				toInclude = false;
	        				break;
	        			}
            		} else {
            			normFieldValue = (intFieldValue - stats.getMin()) / (stats.getMax() - stats.getMin());
            		}
            		if (scale > 0) {
            			stBld.append((int)(scale * normFieldValue)).append(fieldDelimOut);
            		} else {
            			stBld.append(String.format(decFormat, normFieldValue)).append(fieldDelimOut);
            		}
            	}else if (attr.isDouble()) {
            		dblFieldValue = Double.parseDouble(itemValue);
            		if (normalizers.get(i).equals("zScore")) {
	            		normFieldValue = (dblFieldValue - stats.getMean()) / stats.getStdDev();
	        			if (isOutlier()) {
	        				toInclude = false;
	        				break;
	        			}
            		} else {
            			normFieldValue = (dblFieldValue - stats.getMin()) / (stats.getMax() - stats.getMin());
            		}
            		if (scale > 0) {
            			stBld.append(String.format(decFormat, scale * normFieldValue)).append(fieldDelimOut);
            		} else {
            			stBld.append(String.format(decFormat, normFieldValue)).append(fieldDelimOut);
            		}
            		
            	} else {
        			stBld.append(itemValue).append(fieldDelimOut);
        		}
            }
            
            if (toInclude) {
            	outVal.set(stBld.substring(0, stBld.length() -1));
            	context.write(NullWritable.get(), outVal);
            }
        }
        
        /**
         * @return
         */
        private boolean isOutlier() {
        	boolean outlier = false;
			if (outlierTruncationLevel > 0) {
				if (Math.abs(normFieldValue) > outlierTruncationLevel) {
					outlier = true;
				} 
			}
        	return outlier;
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericalAttrNormalizer(), args);
        System.exit(exitCode);
	}

}
