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
import org.chombo.transformer.AttributeTransformer;
import org.chombo.transformer.TransformerFactory;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.ProcessorAttribute;
import org.chombo.util.ProcessorAttributeSchema;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * Transforms attributes based on plugged in transformers for different attributes.
 * This class and the nested mapper calss need to be extended to do any useful work
 * @author pranab
 *
 */
public class Transformer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Transformer  MR";
        job.setJobName(jobName);
        job.setJarByClass(Transformer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(Transformer.TransformerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @param jobName
	 * @param jobClass
	 * @param mapClass
	 * @param args
	 * @return
	 * @throws Exception
	 */
	protected  int start(String jobName, Class<?> jobClass, Class<? extends Mapper> mapClass, String[] args)  
		throws Exception {
        Job job = new Job(getConf());
        job.setJobName(jobName);
        job.setJarByClass(jobClass);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(mapClass);
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
	public static class TransformerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
		private Map<Integer, List<AttributeTransformer>> transformers = new HashMap<Integer, List<AttributeTransformer>>();
		private List<AttributeTransformer> generators = new ArrayList<AttributeTransformer>();
		private AttributeTransformer transformer;
		private String[] transformedValues;
        private String[] items;
        private String[] itemsOut;
        private String[] singleTransformedValue = new String[1];
        private List<AttributeTransformer>  transformerList;
        private String source;
        private ProcessorAttributeSchema transformerSchema;
        private Config transformerConfig;
        private boolean configDriven;
        private int fieldOrd;;
       
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim", ",");
        	
        	//transformer schema
        	configDriven = config.get("transformer.schema.file.path") != null;
        	if (configDriven) {
        		//schema
	        	transformerSchema = Utility.getProcessingSchema(config, "transformer.schema.file.path");
	        	transformerSchema.validateTargetAttributeMapping();
	        	
	        	//transformer config
	        	transformerConfig = Utility.getHoconConfig(config, "transformer.config.file.path");
	        	
	        	//intialize transformer factory
	        	TransformerFactory.initialize( config.get( "custom.trans.factory.class"), transformerConfig);

	        	//build transformers
	        	AttributeTransformer attrTrans;
	        	for (ProcessorAttribute prAttr : transformerSchema.getAttributes()) {
	        		fieldOrd = prAttr.getOrdinal();
	        		if (null != prAttr.getTransformers()) {
	        			for (String tranformerTag  : prAttr.getTransformers() ) {
	        				attrTrans = TransformerFactory.createTransformer(tranformerTag, prAttr, transformerConfig);
	        				registerTransformers(fieldOrd, attrTrans);
	        			}
	        		}
	        	}
	        	
	        	//build generators
	        	if (null != transformerSchema.getAttributeGenerators()) {
		        	for (ProcessorAttribute prAttr : transformerSchema.getAttributeGenerators()) {
		        		for (String tranformerTag  : prAttr.getTransformers() ) {
		        			attrTrans = TransformerFactory.createTransformer(tranformerTag, prAttr, transformerConfig);
		        			registerGenerators(attrTrans);
		        		}
		        	}
	        	}
	        	
	        	//output
	        	itemsOut = new String[transformerSchema.findDerivedAttributeCount()];
        	}
       }
        
        
        /**
         * @param fieldOrd
         * @param transformer
         */
        protected void registerTransformers(int fieldOrd, AttributeTransformer...  transformer) {
        	List<AttributeTransformer> transList = transformers.get(fieldOrd);
        	if (null == transList) {
        		transList = new ArrayList<AttributeTransformer>();
        		transformers.put(fieldOrd, transList);
        	}
        	
        	//add all
        	for (AttributeTransformer trans :  transformer) {
        		transList.add(trans);
        	}
        }

        /**
         * @param transformer
         */
        protected void registerGenerators( AttributeTransformer...  transformer) {
        	//add all
        	for (AttributeTransformer trans :  transformer) {
        		generators.add(trans);
        	}
        }      
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            stBld.delete(0, stBld.length());
            if (configDriven) {
            	//using configuration based transformers
            	
            	//transformers
            	getTranformedAttributes(transformerSchema.getAttributes(), true);
	        	
            	//generators
	           	getTranformedAttributes(transformerSchema.getAttributeGenerators(), false);
	           	
	            outVal.set(Utility.join(itemsOut, fieldDelimOut));
				context.write(NullWritable.get(), outVal);
            } else {
            	//using directly built transformers
	            for (int i = 0; i < items.length; ++i) {
	            	//either transform or pass through
	            	transformerList = transformers.get(i);
	            	int t = 0;
	            	source = items[i];
	            	
            		//skip field if no transformers defined
	            	if (null != transformerList) {
		            	//all transformers
		            	for (AttributeTransformer trans :  transformerList) {
			        		if (null !=trans) {
			        			transformedValues = trans.tranform(source);
			        			if (transformerList.size() > 1 && t <  transformerList.size() -1 && transformedValues.length > 1 ) {
			        				//only last transformer is allowed to emit multiple values
			        				throw new  IllegalStateException("for cascaded transformeronly last transformer is allowed to emit multiple values");
			        			}
			        		} else {
			        			singleTransformedValue[0] = source;
			        			transformedValues =  singleTransformedValue;
			        		}
			        		
			        		source = transformedValues[0];
			        		++t;
			            }
		            	
		        		//add to output
		        		if (null != transformedValues) {
		        			for (String transformedValue :  transformedValues) {
		        				stBld.append(transformedValue).append(fieldDelimOut);
		        			}
		        		}
	            	}
	            }
	            outVal.set(stBld.substring(0, stBld.length() -1));
				context.write(NullWritable.get(), outVal);
	        }
        }
        
        /**
         * @param prAttrs
         * @param isTransformer
         */
        private void getTranformedAttributes(List<ProcessorAttribute> prAttrs, boolean isTransformer) {
        	for (ProcessorAttribute prAttr : prAttrs) {
        		if (null == prAttr.getTargetFieldOrdinals()) {
        			//field is omitted from output
        			continue;
        		}
        		if (isTransformer) {
        			fieldOrd = prAttr.getOrdinal();
                	source = items[fieldOrd];
        			transformerList = transformers.get(fieldOrd);
        		} else {
        			source = null;
        			transformerList = generators;
        		}
        		
        		//chained transformation
            	int t = 0;
            	transformedValues = null;
            	if (null != transformerList) {
	            	for (AttributeTransformer trans :  transformerList) {
	        			transformedValues = trans.tranform(source);
	        			if (transformerList.size() > 1 && t <  transformerList.size() -1 && transformedValues.length > 1 ) {
	        				//only last transformer is allowed to emit multiple values
	        				throw new  IllegalStateException("for cascaded transformeronly last transformer is allowed to emit multiple values");
	        			}
		        		source = transformedValues[0];
	        			++t;
	            	}
            	}
            	
            	//no transformers or generators
            	if (null  == transformedValues) {
            		transformedValues = new String[1];
            		transformedValues[0] = source;
            	}
            	
            	//check output size with target attribute count
            	if (transformedValues.length != prAttr.getTargetFieldOrdinals().length) {
            		throw new IllegalStateException("transformed output size does not match with target attribute count");
            	}
            	
            	//populated target attributes
            	t = 0;
            	for (int targetOrd : prAttr.getTargetFieldOrdinals()) {
            		itemsOut[targetOrd] = transformedValues[t];
            		++t;
            	}
        	}
        	
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class NullTransformer extends AttributeTransformer {
		public NullTransformer() {
			super(1);
		}

		@Override
		public String[] tranform(String value) {
			transformed[0] = null;
			return transformed;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Transformer(), args);
        System.exit(exitCode);
	}
	
}
