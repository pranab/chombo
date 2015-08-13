package org.chombo.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.chombo.util.Attribute;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Counts unique values for fields except for double and text type
 * @author pranab
 *
 */
public class UniqueCounter  extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "MR for unique value counting for various fields ";
        job.setJobName(jobName);
        
        job.setJarByClass(UniqueCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(UniqueCounter.CounterMapper.class);
        job.setReducerClass(UniqueCounter.CounterReducer.class);
        job.setCombinerClass(UniqueCounter.CounterCombiner.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("unc.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class CounterMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
		private IntWritable outKey = new IntWritable();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String[] items;
        private String fieldDelimRegex;
        private GenericAttributeSchema schema;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	schema = Utility.getGenericAttributeSchema(config,  "schema.file.path");
        	attributes = Utility.intArrayFromString(config.get("attr.list"),configDelim );
    		List<Attribute> attrs = schema.getQuantAttributes(Attribute.DATA_TYPE_CATEGORICAL, Attribute.DATA_TYPE_DATE, 
    				Attribute.DATA_TYPE_INT, Attribute.DATA_TYPE_LONG, Attribute.DATA_TYPE_STRING);
        	if (null == attributes) {
        		//use schema
        		attributes = new int[attrs.size()];
        		for (int i = 0; i < attrs.size(); ++i) {
        			attributes[i] = attrs.get(i).getOrdinal();
        		}
        	} else {
        		for (int ord : attributes ) {
        			boolean found = false;
        			for (Attribute attr : attrs) {
        				if (attr.getOrdinal() == ord) {
        					found = true;
        					break;
        				}
        			}
        			
        			if (!found) {
        				throw new IllegalArgumentException("Only quant attributes except for type double and text allowed");
        			}
        		}
        	}
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	for (int attr : attributes) {
        		outVal.initialize();
        		outKey.set(attr);
        		outVal.add(items[attr]);
            	context.write(outKey, outVal);
        	}
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class CounterCombiner extends Reducer<IntWritable, Tuple, IntWritable, Tuple> {
		private Tuple outVal = new Tuple();
		private Set<String> uniqueValues = new HashSet<String>();
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(IntWritable  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	uniqueValues.clear();
        	outVal.initialize();
    		for (Tuple val : values) {
    			for (int i =0; i < val.getSize(); ++i) {
    				uniqueValues.add(val.getString(i));
    			}
    		}
    		
    		for (String uniqueValue : uniqueValues) {
    			outVal.add(uniqueValue);
    		}
        	context.write(key, outVal);
        }	
	}

	   /**
  * @author pranab
  *
  */
 public static class CounterReducer extends Reducer<IntWritable, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private Set<String> uniqueValues = new HashSet<String>();
		private boolean outputCount;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	outputCount = config.getBoolean("output.count", true);
		}
	   	
		/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(IntWritable key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	uniqueValues.clear();
    		for (Tuple val : values) {
    			for (int i =0; i < val.getSize(); ++i) {
    				uniqueValues.add(val.getString(i));
    			}
    		}
    		
    		if (outputCount) {
    			//count
    			outVal.set("" +  key.get() + fieldDelim + uniqueValues.size());
    		} else {
    			//actual values
	       		stBld.delete(0, stBld.length());
	       		stBld.append(key.get()).append(fieldDelim);
	    		for (String uniqueValue : uniqueValues) {
	    			stBld.append(uniqueValue).append(fieldDelim);
	    		}    	
	    		outVal.set(stBld.substring(0, stBld.length() -1));
    		}
			context.write(NullWritable.get(), outVal);
    	}		
 	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UniqueCounter(), args);
		System.exit(exitCode);
	}

 
}
