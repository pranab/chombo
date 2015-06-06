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
import java.io.OutputStream;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.AttributeSchema;
import org.chombo.util.Utility;
import org.chombo.validator.InvalidData;
import org.chombo.validator.Validator;
import org.chombo.validator.ValidatorFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class ValidationChecker extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "ValidationChecker  MR";
        job.setJobName(jobName);
        job.setJarByClass(ValidationChecker.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(ValidationChecker.ValidationMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        //create invalid data report file
        OutputStream os = Utility.getCreateFileOutputStream(job.getConfiguration(), "invalid.data.file.path");
        os.close();
        
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
        private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
        private String[] items;
        private AttributeSchema schema;
        private Map<Integer, List<Validator>> validators = new HashMap<Integer, List<Validator>>();
        private List<InvalidData> invalidDataList = new ArrayList<InvalidData>();
        private boolean filterInvalidRecords;
        private String fieldValue;
        private boolean valid;
        private String invalidDataFilePath;
        private Map<String, Object> validatorContext = new HashMap<String, Object>(); 
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim", ",");
        	filterInvalidRecords = config.getBoolean("filter.invalid.records", true);
        	invalidDataFilePath = config.get("invalid.data.file.path");
        	
        	//schema
        	InputStream is = Utility.getFileStream(config,  "schema.file.path");
        	ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(is, AttributeSchema.class);

            //build validator objects
            int[] ordinals  = schema.getAttributeOrdinals();
            boolean statsIntialized = false;
            for (int ord : ordinals ) {
            	String key = "validator." + ord;
            	String validatorString = config.get(key);
            	if (null != validatorString ) {
            		List<Validator> validatorList = new ArrayList<Validator>();  
            		validators.put(ord, validatorList);
            		String[] valTags = validatorString.split(fieldDelimOut);
            		for (String valTag :  valTags) {
            			if (valTag.equals("statBasedRange")) {
            				if (!statsIntialized) {
            					List<String[]> statContent = Utility.parseFileLines(config, "stat.file.path",  fieldDelimOut);
            					int ordFieldIndex = config.getInt("stat.ord.index", -1);
            					int meanFieldIndex = config.getInt("stat.mean.index", -1);
            					int stdDevFieldIndex = config.getInt("stat.stdDev.index", -1);
            					for (String[] items :  statContent) {
            						int statOrd = Integer.parseInt(items[ordFieldIndex]);
            						double mean = Double.parseDouble(items[meanFieldIndex]);
            						double stdDev = Double.parseDouble(items[stdDevFieldIndex]);
            						validatorContext.put("mean:" + statOrd,  mean);
            						validatorContext.put("stdDev:" + statOrd,  stdDev);
            					}
            					statsIntialized = true;
            				}
            				validatorList.add(ValidatorFactory.create(valTag, ord, schema,validatorContext));
            			} else {
            				validatorList.add(ValidatorFactory.create(valTag, ord, schema,null));
            			}
            		}
            	}
            }
       }
        
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
        	Configuration config = context.getConfiguration();
            OutputStream os = Utility.getAppendFileOutputStream(config, "invalid.data.file.path");
			
            for (InvalidData invalidData : invalidDataList ) {
            	byte[] data = invalidData.toString().getBytes();
            	os.write(data);
            }
            os.flush();
            os.close();
		}


		/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            stBld.delete(0, stBld.length());
            valid = true;
            InvalidData invalidData = null;
            for (int i = 0; i < items.length; ++i) {
            	List<Validator> validatorList = validators.get(i);
            	fieldValue = items[i]; 
            	if(null != validatorList) {
            		for (Validator validator : validatorList) {
            			valid = validator.isValid(fieldValue);
            			if (!valid) {
            				if (null == invalidData) {
            					invalidData = new InvalidData(value.toString());
            					invalidDataList.add(invalidData);
            				}
            				invalidData.addValidationFailure(i, validator.getTag());
            			}
            		}
            	}
            }
            
            if (valid || !filterInvalidRecords ) {
            	outVal.set(value.toString());
            	context.write(NullWritable.get(), outVal);
            }
            
        }
	}	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ValidationChecker(), args);
        System.exit(exitCode);
	}
	
}
