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
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class NumericSorter  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Sorting by numeric field MR";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericSorter.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(NumericSorter.NumericSorterMapper.class);
        int numReducer = job.getConfiguration().getInt("nus.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class NumericSorterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable outKey = new LongWritable();
		private int sortField;
        private String fieldDelimRegex;
        private boolean sortOrderAscending;
        private long sortFieldVal;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	sortField = context.getConfiguration().getInt("nus.sort.field", 0);
        	sortOrderAscending = context.getConfiguration().getBoolean("nus.sort.order.ascending", true);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", ",");
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            sortFieldVal = Long.parseLong(items[sortField]);
            if (!sortOrderAscending) {
            	sortFieldVal = sortFieldVal == 0 ? Long.MAX_VALUE :  (Long.MAX_VALUE  - 1)  / sortFieldVal;
            	outKey.set( sortFieldVal );
            }
            
        	outKey.set(sortFieldVal);
			context.write(outKey, value);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class NumericSorterReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
    	
    	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
        	for (Text value : values){
 				context.write(NullWritable.get(), value);
        	}    		
    	}
    	
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericSorter(), args);
        System.exit(exitCode);
	}
}
