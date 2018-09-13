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
import org.chombo.stats.MedianStatsManager;
import org.chombo.stats.NumericalAttrStatsManager;
import org.chombo.util.BasicUtils;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.ProcessorAttributeSchema;
import org.chombo.util.Utility;
import org.chombo.validator.InvalidData;
import org.chombo.validator.Validator;
import org.chombo.validator.ValidatorFactory;

import com.typesafe.config.Config;

/**
 * Data validator. Multiple out of the box validators can be configured for each field. Custom validators
 * can also be defined.
 * @author pranab
 *
 */
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
        job.setNumReduceTasks(0);
        
        //create invalid data report file
       OutputStream os = Utility.getCreateFileOutputStream(job.getConfiguration(), "vac.invalid.data.file.path");
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
        private Map<Integer, List<Validator>> validators = new HashMap<Integer, List<Validator>>();
        private List<InvalidData> invalidDataList = new ArrayList<InvalidData>();
        private boolean filterInvalidRecords;
        private String fieldValue;
        private boolean valid;
        private String invalidDataFilePath;
        private Map<String, Object> validatorContext = new HashMap<String, Object>(); 
        private MedianStatsManager medStatManager;
        private int[] idOrdinals;
        private NumericalAttrStatsManager statsManager;
        private ProcessorAttributeSchema validationSchema;      
        private List<Validator> rowValidators = new ArrayList<Validator>();
        private boolean outputValidationFailures;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	filterInvalidRecords = config.getBoolean("vac.filter.invalid.records", true);
        	invalidDataFilePath = config.get("vac.invalid.data.file.path");
        	outputValidationFailures = config.getBoolean("vac.output.validation.failures", true);

           	//record id
        	idOrdinals = Utility.intArrayFromString(config.get("vac.id.field.ordinals"), fieldDelimRegex);
 
        	//schema
        	validationSchema = Utility.getProcessingSchema(config, "vac.validation.schema.file.path");
 
            //validator config
            Config validatorConfig = Utility.getHoconConfig(config, "vac.validator.config.file.path");
            
        	//initialize validator factory
        	ValidatorFactory.initialize(config.get("vac.custom.valid.factory.class"), validatorConfig );

        	//build validator objects
            int[] ordinals  = validationSchema.getAttributeOrdinals();
            

            //validators cpnfig prop file 
            boolean validatorInPropConfig = config.getBoolean("vac.validator.config.present", true);
            
            if (validatorInPropConfig) {
	            //field wise validators
	            for (int ord : ordinals ) {
	            	String key = "vac.validator." + ord;
	            	String validatorString = config.get(key);
	            	if (null != validatorString ) {
	            		String[] valTags = validatorString.split(fieldDelimOut);
	            		createValidators(config, valTags, ord, null);
	            	}
	            }
	            
            	//row wise validators
            	String[] rowValidatorTags = Utility.optionalStringArrayConfigParam(config, "vac.row.validator", Utility.configDelim);
            	if (null != rowValidatorTags) {
            		createRowValidators(config, rowValidatorTags,  validatorConfig);
            	}
            } else  {
                //field wise validators
           		for (ProcessorAttribute prAttr : validationSchema.getAttributes()) {
	        		List<String> validatorTags =  prAttr.getValidators();
	        		if (null != validatorTags) {
	        			String[] valTags = validatorTags.toArray(new String[validatorTags.size()]);
	        			createValidators( config,  valTags,  prAttr.getOrdinal(), validatorConfig);
	        		}
	           	}
           		
           		//row wise validators
           		List<String> rowValidatorTagList = validationSchema.getRowValidators();
           		if (null != rowValidatorTagList) {
        			String[] rowValidatorTags = rowValidatorTagList.toArray(new String[rowValidatorTagList.size()]);
            		createRowValidators(config, rowValidatorTags,  validatorConfig);
           		}
            }
       }
        
        /**
         * @param config
         * @param valTags
         * @param ord
         * @param validatorConfig
         * @throws IOException
         */
        private void createValidators(Configuration config, String[] valTags,  int ord, Config validatorConfig) 
        		throws IOException {
        	//create all validator for  a field
    		List<Validator> validatorList = new ArrayList<Validator>();  
			ProcessorAttribute prAttr = validationSchema.findAttributeByOrdinal(ord);
    		for (String valTag :  valTags) {
    			if (valTag.equals("zscoreBasedRange")) {
    				//z score based
    				getAttributeStats(config, "vac.stat.file.path");
    				validatorList.add(ValidatorFactory.create(valTag, prAttr, validatorContext));
    			} if (valTag.equals("robustZscoreBasedRange")) {
    				//robust z score based
    				getAttributeMeds(config, "vac.med.stat.file.path", "vac.mad.stat.file.path", idOrdinals);
    				validatorList.add(ValidatorFactory.create(valTag, prAttr,validatorContext));
    			} else {
    				//normal
    				Validator validator = ValidatorFactory.create(valTag, prAttr,  validatorConfig);
    				validatorList.add(validator);
    			}
    		}
    		validators.put(ord, validatorList);
        }
        
        /**
         * @param config
         * @param valTags
         * @param validatorConfig
         * @throws IOException
         */
        private void createRowValidators(Configuration config, String[] valTags, Config validatorConfig ) 
        		throws IOException {
        	Validator validator =  null;
        	for (String valTag : valTags) {
        		if (null == validatorConfig) {
        			createRowValidatorContext(config, valTag);
        			validator = ValidatorFactory.create(valTag, validatorContext, fieldDelimRegex);
        		} else {
        			Config thisValidatorConfig = validatorConfig.getConfig(valTag);
        			validator = ValidatorFactory.create(valTag, thisValidatorConfig, fieldDelimRegex);
        		}
        		
        		rowValidators.add(validator);
        	}
        }   
        
        /**
         * @param config
         * @param valTag
         */
        private void createRowValidatorContext(Configuration config, String valTag) {
			validatorContext.clear();
    		if (valTag.equals("notMissingGroup")) {
    			String[] groups = Utility.assertStringArrayConfigParam(config, "vac.missing.value.field.gropus", 
    					Utility.configDelim, "missing missing value field gropus");
    			List<int[]> fieldGroups = new ArrayList<int[]>();
    			for (String group : groups) {
    				int[] groupItems = BasicUtils.intArrayFromString(group, ":");
    				fieldGroups.add(groupItems);
    			}
    			validatorContext.put("fieldGroups", fieldGroups);
    		} else {
    		     throw new IllegalStateException("missing context for row validator " + valTag);
    		}
        }
        
        /**
         * @param config
         * @param statsFilePath
         * @throws IOException
         */
        private void getAttributeStats(Configuration config, String statsFilePath) throws IOException {
        	if (null == statsManager ) {
        		statsManager = new NumericalAttrStatsManager(config, statsFilePath, ",");
        	}
        	validatorContext.clear();
			validatorContext.put("stats",  statsManager);
        }

        /**
         * @param config
         * @param statsFilePath
         * @throws IOException
         */
        private void getAttributeMeds(Configuration config, String medFilePathParam, String madFilePathParam, int[] idOrdinals) 
        	throws IOException {
        	if (null == medStatManager) {
        		medStatManager = new MedianStatsManager(config, medFilePathParam, madFilePathParam,  
        			",",  idOrdinals, false);
        	}
        	validatorContext.clear();
			validatorContext.put("stats",  medStatManager);
        }
        
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
        	Configuration config = context.getConfiguration();
            OutputStream os = Utility.getAppendFileOutputStream(config, "vac.invalid.data.file.path");
			
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
            items  =  value.toString().split(fieldDelimRegex, -1);
            stBld.delete(0, stBld.length());
            valid = true;
            InvalidData invalidData = null;
            
            //field wise validation
            for (int i = 0; i < items.length; ++i) {
            	List<Validator> validatorList = validators.get(i);
            	fieldValue = items[i]; 
            	if(null != validatorList) {
            		for (Validator validator : validatorList) {
            			if (ValidatorFactory.isCustomValidator(validator.getTag())) {
            				//custom validator, pass whole record
            				valid = validator.isValid(value.toString());
            			} else {
            				//pass only field
            				valid = validator.isValid(fieldValue);
            			}
            			
            			//track invalid records
            			if (!valid) {
            				if (null == invalidData) {
            					invalidData = new InvalidData(value.toString(), outputValidationFailures);
            					invalidDataList.add(invalidData);
            				}
            				invalidData.addValidationFailure(i, validator.getTag());
            			}
            		}
            	}
            }
            
            //whole row validator
            for (Validator validator : rowValidators) {
            	valid = validator.isValid(value.toString());
            	if (!valid) {
					if (null == invalidData) {
						invalidData = new InvalidData(value.toString(), outputValidationFailures);
						invalidDataList.add(invalidData);
					}
					invalidData.addRowValidationFailure(validator.getTag());
            	}
           }
            
            //valid or all records
            if (null == invalidData || !filterInvalidRecords ) {
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
