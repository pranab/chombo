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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;
import org.chombo.util.Tuple;

/**
 * @author pranab
 *
 */
public class MultiVarHistogram extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Muti variate histogram MR";
        job.setJobName(jobName);
        
        job.setJarByClass(MultiVarHistogram.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(MultiVarHistogram.HistogramMapper.class);
        job.setReducerClass(MultiVarHistogram.HistogramReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("mvh.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class HistogramMapper extends Mapper<LongWritable, Text, Tuple , IntWritable> {
		private Tuple outKey = new Tuple();
		private IntWritable outVal = new IntWritable(1);
        private String fieldDelimRegex;
        private RichAttributeSchema schema;
        private String keyCompSt;
        private Integer keyCompInt;
        private int count = 0;
        private int numFields;
        
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", ",");
            
        	String filePath = conf.get("mvh.histogram.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, RichAttributeSchema.class);
            
            numFields = schema.getFields().size();
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            if ( items.length  != numFields){
            	context.getCounter("Data", "Invalid").increment(1);
            	return;
            }
            
            outKey.initialize();
            for (RichAttribute field : schema.getFields()) {
            	String	item = items[field.getOrdinal()];
            	if (field.isCategorical()){
            		keyCompSt = item;
            		outKey.add(keyCompSt);
            	} else if (field.isInteger()) {
            		 keyCompInt = Integer.parseInt(item) /  field.getBucketWidth();
            		outKey.add(keyCompInt);
            	} else if (field.isDouble()) {
            		 keyCompInt = ((int)Double.parseDouble(item)) /  field.getBucketWidth();
            		outKey.add(keyCompInt);
            	}
            }
        	context.getCounter("Data", "Processed record").increment(1);
        	//System.out.println( "Processed " + (++count) + " key size: " + outKey.getSize());
			context.write(outKey, outVal);
       }
	}
	
    /**
     * @author pranab
     *
     */
    public static class HistogramReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
    	private Text valueOut = new Text();
    	private int sum;
    	private String fieldDelim ;

        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelim = conf.get("field.delim", ",");
        }    	
        
    	protected void reduce(Tuple key, Iterable<IntWritable> values, Context context)
        	throws IOException, InterruptedException {
           	//System.out.println( "Reducer key size: " + key.getSize());
    		key.setDelim(fieldDelim);
   		    sum = 0;
        	for (IntWritable value : values){
        		sum += value.get();
        	}    		
        	valueOut.set(key.toString() + fieldDelim + sum);
			context.write(NullWritable.get(), valueOut);
    	}
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiVarHistogram(), args);
        System.exit(exitCode);
	}
}
