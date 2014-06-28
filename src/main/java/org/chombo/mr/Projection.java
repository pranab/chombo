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
import java.util.HashSet;
import java.util.Set;

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
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Simple query with projection. Can do group by and order by.  If grouped by can do count or 
 * unique count
 * @author pranab
 *
 */
public class Projection extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Projection  and grouping  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Projection.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        String operation = job.getConfiguration().get("projection.operation",  "project");
        
        if (operation.startsWith("grouping")) {
        	//group by
            job.setMapperClass(Projection.ProjectionMapper.class);
            job.setReducerClass(Projection.ProjectionReducer.class);

            job.setMapOutputKeyClass(Tuple.class);
            job.setMapOutputValueClass(Text.class);

            job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
            
            //order by
        	boolean doOrderBy = job.getConfiguration().getInt("orderBy.field", -1) >= 0;
        	if (doOrderBy) {
                job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
                job.setPartitionerClass(SecondarySort.TupleTextPartitioner.class);
        	}

        } else {
        	//simple projection
            job.setMapperClass(Projection.SimpleProjectionMapper.class);
        }
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SimpleProjectionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private int  keyField;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;

        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	keyField = config.getInt("key.field", 0);
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim", ",");
        	projectionFields = Utility.intArrayFromString(config.get("projection.field"),fieldDelimRegex );
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
	        outVal.set(items[keyField] + fieldDelimOut +  Utility.extractFields(items , projectionFields, 
	        		fieldDelimOut, false));
	        context.write(NullWritable.get(), outVal);
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class ProjectionMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
		private int  keyField;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private int orderByField;
        private boolean groupBy;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	String operation = config.get("projection.operation",  "project");
        	groupBy = operation.startsWith("grouping");
        	
        	keyField = config.getInt("key.field", 0);
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	fieldDelimOut = config.get("field.delim.out", ",");
        	projectionFields = Utility.intArrayFromString(config.get("projection.field"),fieldDelimRegex );
        	orderByField = config.getInt("orderBy.field", -1);
       }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
        	outKey.initialize();
            if (orderByField >= 0) {
            	outKey.add(items[keyField], items[orderByField]);
            } else {
            	outKey.add(items[keyField]);
            }
        	outVal.set( Utility.extractFields(items , projectionFields, fieldDelimOut, false));
        	context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class ProjectionReducer extends Reducer<Tuple, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private boolean countField;
		private Set<String> uniqueValues = new  HashSet<String>();
		private boolean uniqueCount;
		private int count = 0;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", "[]");
        	countField = config.getBoolean("count.field", false);
        	uniqueCount = config.getBoolean("unique.count", false);
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		stBld.delete(0, stBld.length());
    		stBld.append(key.getString(0));
    		if (countField) {
    			if (uniqueCount) {
	    			uniqueValues.clear();
		        	for (Text value : values){
		        		uniqueValues.add(value.toString());
		        	}
	    	   		stBld.append(fieldDelim).append(uniqueValues.size());
    			} else {
    				count = 0;
		        	for (Text value : values){
		        		++count;
		        	}
	    	   		stBld.append(fieldDelim).append(count);
    			}
    		} else {
    			//actual values
	        	for (Text value : values){
	    	   		stBld.append(fieldDelim).append(value);
	        	}    		
    		}
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    	
    }
 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Projection(), args);
        System.exit(exitCode);
	}

}
