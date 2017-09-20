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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.BasicUtils;
import org.chombo.util.Utility;

/**
 * Calculates time rate of change for one or more quantities
 * @author pranab
 *
 */
public class TemporalRate extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Temporal rate calculation for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(TemporalRate.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(TemporalRate.RateMapper.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	/**
	 * @author pranab
	 *
	 */
	public static class RateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelim;
        private String[] items;
        private int[] idOrdinals;
        private boolean quantValueBeginEndSpecified;
        private boolean timeWindowBeginEndSpecified;
        private int[] quantFieldOrdinals;
        private int[] timeWindowFieldOrdinals;
        private String dateTimeFormat;
        private long rateTimeUnitMs;
        private int numQuants;
        private double[] quantValues;
        private long timeWindowSize;
        private int outputPrecision;
		private StringBuilder stBld = new StringBuilder();;

        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelim = config.get("field.delim.out", ",");

        	//data format
        	quantValueBeginEndSpecified = config.getBoolean("ter.quant.value.begin.end.specified", true);
        	timeWindowBeginEndSpecified = config.getBoolean("ter.time.window.begin.end.specified", true);
        	
        	//field ordinals
        	idOrdinals = Utility.assertIntArrayConfigParam(config, "ter.id.ordinals", Utility.configDelim, 
        			"missing id field ordinals");
        	quantFieldOrdinals = Utility.assertIntArrayConfigParam(config, "ter.quant.field.ordinals", Utility.configDelim, 
        			"missing quant field ordinals");
        	timeWindowFieldOrdinals = Utility.assertIntArrayConfigParam(config, "ter.time.window.field.ordinals", Utility.configDelim, 
        			"missing quant field ordinals");
        	
        	//num of quants
        	numQuants = quantValueBeginEndSpecified ? quantFieldOrdinals.length / 2 : quantFieldOrdinals.length;
        	quantValues = new double[numQuants];
        	
        	//date format
        	dateTimeFormat = config.get("ter.date.time.format", BasicUtils.EPOCH_TIME);
        	
        	//rate time unit
        	String rateTimeUnit = config.get("ter.rate.time.unit", BasicUtils.EPOCH_TIME);
        	rateTimeUnitMs = BasicUtils.toEpochTime(rateTimeUnit);
        	
        	outputPrecision = config.getInt("nas.output.prec", 3);
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            //rates
            getQuantValues();
            getTimeWindowSize();
            for (int i = 0; i < numQuants; ++i) {
            	quantValues[i] = quantValues[i] * rateTimeUnitMs / timeWindowSize;
            }
            
    		stBld.delete(0, stBld.length());
    		
    		//ID
    		for (int idOrd : idOrdinals) {
    			stBld.append(items[idOrd]).append(fieldDelim);
    		}
    		
    		//all rates
    		for (double quantVal : quantValues) {
    			stBld.append(BasicUtils.formatDouble(quantVal, outputPrecision)).append(fieldDelim);
    		}
    		
    		outVal.set(stBld.substring(0, stBld.length() -1));
    		context.write(NullWritable.get(), outVal);
        }   
        
        /**
         * 
         */
        private void getQuantValues() {
        	double value = 0;
        	for (int i = 0; i < numQuants; ++i) {
        		if (quantValueBeginEndSpecified) {
        			double beg = Double.parseDouble(items[quantFieldOrdinals[2 * i]]);
        			double end = Double.parseDouble(items[quantFieldOrdinals[2 * i + 1]]);
        			value = end - beg;
        		} else {
        			value = Double.parseDouble(items[quantFieldOrdinals[i]]);
        		}
            	quantValues[i] = value;
        	}
        }
        
        /**
         * @return
         */
        private void getTimeWindowSize() {
        	//long windowSize = 0;
        	if (timeWindowBeginEndSpecified) {
        		if (timeWindowFieldOrdinals.length != 2) {
    				throw new IllegalStateException("invalid number of time window column ordinals");
        		}
        		if (dateTimeFormat.equals(BasicUtils.EPOCH_TIME)) {
        			long beg = Long.parseLong(items[timeWindowFieldOrdinals[0]]);
        			long end = Long.parseLong(items[timeWindowFieldOrdinals[1]]);
        			if (beg > end) {
        				throw new IllegalStateException("invalid begin and end time");
        			}
        			timeWindowSize = end - beg;
        		} else {
        			
        		}
        	} else {
        		if (timeWindowFieldOrdinals.length != 1) {
    				throw new IllegalStateException("invalid number of time window column ordinals");
        		}
        		timeWindowSize = Long.parseLong(items[timeWindowFieldOrdinals[0]]);
        	}
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TemporalRate(), args);
		System.exit(exitCode);
	}

}
