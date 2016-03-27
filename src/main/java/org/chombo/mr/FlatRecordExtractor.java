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
import org.chombo.transformer.JsonFieldExtractor;
import org.chombo.transformer.MultiLineFlattener;
import org.chombo.transformer.RawAttributeSchema;
import org.chombo.transformer.UnstructuredFieldExtractor;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Generates flat record with delemeter separated fields from unstructured data which
 * could be multi line
 * @author pranab
 *
 */
public class FlatRecordExtractor extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "FlatRecordExtractor  MR";
        job.setJobName(jobName);
        job.setJarByClass(FlatRecordExtractor.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(FlatRecordExtractor.ExtractionMapper.class);
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
	public static class ExtractionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private String[] items;
        private String[] itemsOut;
        private RawAttributeSchema rawSchema;
        private String rawLine;
        private int exAttrOffset;
        private int rawAttrIndex;
        private int exAttrCount;
        private MultiLineFlattener flattener;
        private UnstructuredFieldExtractor fieldExtractor;
        private boolean valid;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("fre.raw.field.delim.regex");
        	fieldDelimOut = config.get("field.delim", ",");
        	
        	//schema
        	InputStream is = Utility.getFileStream(config, "fre.raw.schema.file.path");
        	ObjectMapper mapper = new ObjectMapper();
        	rawSchema = mapper.readValue(is, RawAttributeSchema.class);
        	
        	//output
        	itemsOut = new String[rawSchema.getDerivedAttributeCount()];
        	if (null == fieldDelimRegex) {
        		items = new String[1];
        	}
        	
        	//field extractor
        	boolean failOnInvalid = config.getBoolean("fre.fail.on.invalid", true);
        	fieldExtractor = new UnstructuredFieldExtractor(rawSchema, failOnInvalid);
        	
        	//record type
        	if (rawSchema.getRecordType().equals(RawAttributeSchema.REC_MULTI_LINE)) {
        		flattener = new MultiLineFlattener(rawSchema);
        	}
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (null != flattener) {
            	rawLine = flattener.processCleanup();
            	emitOutput(context);
            }
        }
        
		/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            
            if (null != flattener) {
            	//multi line
            	rawLine  = flattener.processRawLine(value.toString());
            } else {
            	//single line
            	rawLine  =  value.toString();
            }
            
            if (null != rawLine) {
            	emitOutput(context);
            }
        }      
        
        /**
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void emitOutput(Context context) throws IOException, InterruptedException {
            if (null != fieldDelimRegex) {
            	items = rawLine.split(fieldDelimRegex);
            } else {
            	items[0] = rawLine;
            }
            
            //generate extracted attributes
            rawAttrIndex = 0;
            exAttrOffset = 0;
            valid = true;
            for (String item : items) {
            	exAttrCount = fieldExtractor.extractAttributes(rawAttrIndex, item, itemsOut, exAttrOffset);
            	if (exAttrCount > 0) {
            		++rawAttrIndex;
            		exAttrOffset += exAttrCount;
            	} else {
            		valid = false;
            		break;
            	}
            }
            
            if (valid) {
            	outVal.set(Utility.join(itemsOut, fieldDelimOut));
            	context.write(NullWritable.get(), outVal);
            }
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FlatRecordExtractor(), args);
        System.exit(exitCode);
	}
	
}
