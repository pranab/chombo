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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Pair;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Perform set operation like intersection and union between 2 keyed sets, Maintains list
 * like order
 * @author pranab
 *
 */
public class SetMerger extends Configured implements Tool {
	private static String configDelim = ",";
	private static String OP_INTERSECT = "intersect";
	private static String OP_UNION = "union";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "SetMerger  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SetMerger.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        
        job.setMapperClass(SetMerger.MergerMapper.class);
        job.setReducerClass(SetMerger.MergerReducer.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("sem.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class MergerMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int  keyLen;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private boolean isFirstTypeSplit;
        private int subKey;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	String firstTypePrefix = config.get("sem.first.type.prefix", "first");
        	isFirstTypeSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(firstTypePrefix);
        	subKey = isFirstTypeSplit ? 0 : 1;
        	keyLen = Utility.assertIntConfigParam(config, "sem.key.length", "missing key length");
        }   
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            //beginning keyLen fields constitute key
            Utility.createStringTupleFromBegining(items, keyLen, outKey);
            outKey.add(subKey);
            Utility.createStringTupleFromEnd(items, keyLen, outVal);
            context.write(outKey, outVal);
        }        
	}

    /**
     * @author pranab
     *
     */
    public static class MergerReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
		private String fieldDelimOut;
		private String operation;
		private boolean orderByFirst;
		private Pair<List<String>, List<String>> records = new Pair<List<String>, List<String>>(
				new ArrayList<String>(), new ArrayList<String>());
		private List<String> result = new ArrayList<String>();
		private List<String> primary;
		private List<String> secondary;
		private int retainCount;
		private List<String> reatinedResult;
		private boolean outputFormatCompact;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	operation = Utility.assertStringConfigParam(config, "sem.merge.operation", "missing set merge operation");
        	orderByFirst = config.getBoolean("sem.order.by.first.set", true);
        	retainCount = config.getInt("sem.retain.count", -1);
        	outputFormatCompact = config.getBoolean("sem.output.format.compact", true);
        }	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextInt key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	result.clear();
        	records.getLeft().clear();
        	records.getRight().clear();
        	int count = 0;
        	for (Tuple value : values){
        		if (count == 0) {
        			value.tupleAsList(records.getLeft());
        		} else {
        			value.tupleAsList(records.getRight());
        		}
        		++count;
        	}
        	if (count > 2) {
        		throw new IllegalStateException("can operate on 2 sets only");
        	}
        	
        	//primary and secondary list
    		if(orderByFirst) {
    			primary = records.getLeft();
    			secondary = records.getRight();
    		} else {
    			primary = records.getRight();
    			secondary = records.getLeft();
    		}
    		
    		//operation
        	if (operation.equals(OP_INTERSECT)) {
        		for (String val : primary) {
        			if (secondary.contains(val)) {
        				result.add(val);
        			}
        		}
        		//truncate by retain count
        		reatinedResult = retainCount > 0 ? result.subList(0, retainCount) : result;
        	} else if (operation.equals(OP_UNION)) {
        		if (retainCount > 0) {
        			//retail half of retain count form each list
        			result.addAll(primary.subList(0, retainCount/2));
        			result.addAll(secondary.subList(0, retainCount/2));
        			reatinedResult = result;
        		} else {
        			//retail all form each list
        			result.addAll(primary);
        			result.addAll(secondary);
        			reatinedResult = result;
        		}
        	} else {
        		throw new IllegalStateException("invalid set operation");
        	}
        	
        	//emit
        	String keyStr = key.toString();
        	if (outputFormatCompact) {
        		outVal.set(keyStr + fieldDelimOut + Utility.join(reatinedResult, fieldDelimOut));
    			context.write(NullWritable.get(), outVal);
        	} else {
        		for (String val : reatinedResult) {
            		outVal.set(keyStr + fieldDelimOut + val);
        			context.write(NullWritable.get(), outVal);
        		}
        	}
        }
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SetMerger(), args);
        System.exit(exitCode);
	}
    
}
