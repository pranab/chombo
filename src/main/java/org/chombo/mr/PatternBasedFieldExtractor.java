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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.chombo.util.Utility;

/**
 * Extract field based on regex with multiple groups
 * @author pranab
 *
 */
public class PatternBasedFieldExtractor extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Pattern based field extractor  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(PatternBasedFieldExtractor.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(PatternBasedFieldExtractor.ExtractorMapper.class);

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
	public static class ExtractorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal  = new Text();
		private String fieldDelim;
		private int numFields;
		private Pattern pattern;
		private Matcher matcher;
		private StringBuilder stBld = new  StringBuilder();
		private int dateFieldIndex;
		private SimpleDateFormat dateFormatter;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
	    	fieldDelim = conf.get("field.delim.out", ",");
	    	String regEx = Utility.assertConfigParam(conf, "pbfe.extractor.regex","extractor regex must be provided");
	    	numFields =  Utility.assertIntConfigParam(conf, "pbfe.num.fields", "number of groups in regex must be provided");
			pattern = Pattern.compile(regEx);
			
			dateFieldIndex = conf.getInt("pbfe.date.field.index", -1);
			if (dateFieldIndex >= 0) {
				String dateFormat = Utility.assertConfigParam(conf, "date.format","date format must be provided");
				dateFormatter =  new SimpleDateFormat(dateFormat);
			}
        }   
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            stBld.delete(0, stBld.length());
            matcher = pattern.matcher(value.toString());
            
			if (matcher.matches()) {
				boolean found = true;
				for (int i = 0; i < numFields; ++i) {
			        String extracted = matcher.group(i+1);
			        if(extracted != null) {
			        	if (i == dateFieldIndex) {
			        		//convert to epoch time
			        		try {
								Date date = dateFormatter.parse(extracted);
								extracted = "" + (date.getTime() / 1000);
							} catch (ParseException e) {
								throw new IllegalArgumentException("ivalid date format");
							}
			        	}
			        	 stBld.append(extracted).append(fieldDelim);
			        } else {
			        	found = false;
			        	break;
			        }
			    }
				
				if (found) {
					outVal.set( stBld.substring(0, stBld.length() -1));
	        		context.write(NullWritable.get(),outVal);
				}
			}            
        }      
        
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PatternBasedFieldExtractor(), args);
        System.exit(exitCode);
	}
	
}
