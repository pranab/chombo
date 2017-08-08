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
import java.util.Arrays;
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
import org.chombo.transformer.JsonComplexFieldExtractor;
import org.chombo.transformer.MultiLineJsonFlattener;
import org.chombo.transformer.RawAttributeSchema;
import org.chombo.util.BasicUtils;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Creates flat record out of JSON. Uses JSON paths to extract nested fields
 * @author pranab
 *
 */
public class FlatRecordExtractorFromJson extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "FlatRecordExtractorFromJson  MR";
        job.setJobName(jobName);
        job.setJarByClass(FlatRecordExtractorFromJson.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(FlatRecordExtractorFromJson.ExtractionMapper.class);
    	job.setReducerClass(FlatRecordExtractorFromJson.ExtractionReducer.class);

    	job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        
        int numReducer = job.getConfiguration().getInt("frej.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * Mapper for  attribute transformation
	 * @author pranab
	 *
	 */
	public static class ExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outVal = new Text();
        private String fieldDelimOut;
        private RawAttributeSchema rawSchema;
        private String jsonString;
        private JsonComplexFieldExtractor fieldExtractor;
        private MultiLineJsonFlattener flattener;
        private boolean normalize;
        private  String baseKey = BasicUtils.generateId();
        private int keyIndex = 1000000;
        private String thisKey;
        private boolean failOnInvalid;
        private String defaultValue;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimOut = config.get("field.delim", ",");
        	
        	//schema
        	InputStream is = Utility.getFileStream(config,  "frej.schema.file.path");
        	ObjectMapper mapper = new ObjectMapper();
        	rawSchema = mapper.readValue(is, RawAttributeSchema.class);
        	
        	failOnInvalid = config.getBoolean("frej.fail.on.invalid", true);
        	normalize = config.getBoolean("frej.normalize.output", true);
        	fieldExtractor = new JsonComplexFieldExtractor(failOnInvalid, normalize);
        	if (!failOnInvalid) {
        		defaultValue = config.get("frej.default.value");
        		if (null != defaultValue) {
        			fieldExtractor.withDefaultValue(defaultValue);
        		}
        	}
        	
        	//ID field
        	//String idFieldPath = config.get("frej.id.attr.path");
        	String[] idFieldPaths = Utility.optionalStringArrayConfigParam(config, "frej.id.attr.paths", Utility.configDelim);
        	if (null != idFieldPaths) {
        		List<String> idFieldPathList = Arrays.asList(idFieldPaths);
        		fieldExtractor.withIdFieldPaths(idFieldPathList);
        	} else {
        		if (normalize) {
        			fieldExtractor.withAutoIdGeneration();
        		}
        	}
        	
        	//record type
        	if (rawSchema.getRecordType().equals(RawAttributeSchema.REC_MULTI_LINE_JSON)) {
        		flattener = new MultiLineJsonFlattener();
        	}

        }
        
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			if (!failOnInvalid) {
				System.out.println("**num of records:" + fieldExtractor.getTotalRecordsCount());
				System.out.println("**num of skipped records:" + fieldExtractor.getSkippedRecordsCount());
				if (null != defaultValue) {
					System.out.println("**num cases with default values:" + fieldExtractor.getDefaultValueCount());
				} 
			}
		}

		/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	if (null != flattener) {
        		//multi ine
        		jsonString = flattener.processRawLine(value.toString());
        	} else {
        		//single line
        		jsonString = value.toString();
        	}
        	
        	if (null != jsonString && fieldExtractor.extractAllFields(jsonString, rawSchema.getJsonPaths())) {
        		//there will be multiple records if there are child objects and result are normalized
        		if (!normalize) {
        			//de normalized
        			++keyIndex;
        			thisKey = baseKey + "-" + keyIndex;
	        		List<String[]> records = fieldExtractor.getExtractedRecords();
	        		for (String[] record : records) {
	        			outKey.set(thisKey);
	        			outVal.set(BasicUtils.join(record, fieldDelimOut));
	        			context.write(outKey, outVal);
	        		}
        		} else  {
        			//parent record, use first field which is entity type as key
        			String[] parentRec = fieldExtractor.getExtractedParentRecord();
        			outKey.set(parentRec[0]);
        			outVal.set(BasicUtils.join(parentRec, 1, fieldDelimOut));
        			context.write(outKey, outVal);
        			
        			//all child records, use first field which is entity type as key
        			Map<String, List<String[]>> childRecMap = fieldExtractor.getExtractedChildRecords();
        			for (String child : childRecMap.keySet()) {
        				List<String[]> childRecs = childRecMap.get(child);
    	        		for (String[] record : childRecs) {
    	        			outKey.set(record[0]);
    	        			outVal.set(BasicUtils.join(record, 1, fieldDelimOut));
    	        			context.write(outKey, outVal);
    	        		}
        			}
        		}
        	}
        }        
	}
	
    /**
     * @author pranab
     *
     */
    public static class ExtractionReducer extends Reducer<Text, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private boolean outputEntityName;
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            fieldDelim = config.get("field.delim.out", ",");
            boolean normalize = config.getBoolean("frej.normalize.output", true);
            if (normalize) {
            	outputEntityName = config.getBoolean("frej.output.entity.name", false);
            }
        }	
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Text key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		if (outputEntityName) {
    			outVal.set(key.toString());
    			context.write(NullWritable.get(), outVal);
    		}
    		
        	for (Text value : values){
    			outVal.set(value.toString());
    			context.write(NullWritable.get(), outVal);
        	}
    	}		
    }	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FlatRecordExtractorFromJson(), args);
        System.exit(exitCode);
	}
	
}
