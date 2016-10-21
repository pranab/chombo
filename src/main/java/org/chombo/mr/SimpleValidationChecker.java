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
import java.text.SimpleDateFormat;

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
import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;
import org.chombo.util.Utility;

/**
 * Does simple validation check on data.
 * @author pranab
 *
 */
public class SimpleValidationChecker extends Configured implements Tool {
	private static final String INVALID_MARKER = "[x]";
	
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "FormatChecker  MR";
        job.setJobName(jobName);
        job.setJarByClass(SimpleValidationChecker.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(SimpleValidationChecker.ValidationMapper.class);
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
	public static class ValidationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String subFieldDelimRegex;
        private String fieldDelimOut;
        private String[] items;
        private String[] fieldTypes;
        private int numFields;
        private boolean valid;
        private String[] invalidRec;
        private int samplingInterval; 
        private int recCount;
        private int nextSample;
        private String invalidFieldMarker;
        private String invalidRecMarker;
        private SimpleDateFormat dateFormatter;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	subFieldDelimRegex = config.get("sub.field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	fieldTypes = Utility.assertStringArrayConfigParam(config, "foc.field.types", Utility.DEF_FIELD_DELIM, 
        			"missing field types");
        	numFields = fieldTypes.length;
        	invalidRec = new String[numFields];
        	float samplingFraction = config.getFloat("foc.sampling.fraction", (float)-1.0);
        	samplingInterval = (int)(1 / samplingFraction); 
        	invalidFieldMarker = config.get("foc.invalid.field.marker", "[x]");
        	invalidRecMarker = config.get("foc.invalid.rec.marker", "[xx]");
        	String dateFormatSt = config.get("foc.date.formatter");
        	if (null != dateFormatSt) {
        		dateFormatter = new SimpleDateFormat(dateFormatSt);
        	}
        }
        
		/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	//sub sampling
        	if (samplingInterval > 0 && recCount < nextSample) {
        		++recCount;
        		return;
        	}
        	
            items  =  value.toString().split(fieldDelimRegex, -1);
            valid = true;
            if (items.length != numFields) {
            	//if field count does not match, individual fields are not checked
            	valid = false;
        		outVal.set(invalidRecMarker + BasicUtils.join(items, fieldDelimOut));
            } else {
            	//check fields
            	for (int i = 0 ; i < numFields; ++i) {
            		if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_INT)) {
            			if (BasicUtils.isInt(items[i])) {
            				invalidRec[i] = items[i];
            			} else {
            				handleInvalid(i);
            			}
            		} else if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_LONG)) {
            			if (BasicUtils.isLong(items[i])) {
            				invalidRec[i] = items[i];
            			} else {
            				handleInvalid(i);
            			}
            		} else if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_DOUBLE)) {
            			if (BasicUtils.isDouble(items[i])) {
            				invalidRec[i] = items[i];
            			} else {
            				handleInvalid(i);
            			}
            		} else if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_STRING_COMPOSITE)) {
            			if (BasicUtils.isComposite(items[i], subFieldDelimRegex)) {
            				invalidRec[i] = items[i];
            			} else {
            				handleInvalid(i);
            			}
            		} else if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_DATE)) {
            			if (null != dateFormatter) {
	            			if (BasicUtils.isDate(items[i], dateFormatter)) {
	            				invalidRec[i] = items[i];
	            			} else {
	            				handleInvalid(i);
	            			}
            			} else  {
            				invalidRec[i] = items[i];
            			}
            		} else if (fieldTypes[i].equals(BaseAttribute.DATA_TYPE_STRING)) {
        				invalidRec[i] = items[i];
            		} else {
            			throw new IllegalStateException("invalid data type");
            		}
            	}
            	
            	if (!valid) {
            		outVal.set(BasicUtils.join(invalidRec, fieldDelimOut));
            	}
            }
            
            //set next sample
        	if (samplingInterval > 0 && recCount == nextSample) {
        		int nextSampleInterval = (int)(samplingInterval * ( 1 + Math.random() / 4));
        		nextSample += nextSampleInterval;
        		++recCount;
        	}
            
            if (!valid) {
            	//emit
				context.write(NullWritable.get(), outVal);
            }
        }
        
        /**
         * @param fieldOrd
         */
        private void handleInvalid(int fieldOrd) {
			valid = false;
			invalidRec[fieldOrd] = invalidFieldMarker + items[fieldOrd];
        }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SimpleValidationChecker(), args);
        System.exit(exitCode);
	}
	
	
}
