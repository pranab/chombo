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
import org.chombo.util.BasicUtils;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class RecordSimilarity extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Similarity between records";
        job.setJobName(jobName);
        
        job.setJarByClass(RecordSimilarity.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(RecordSimilarity.SimilarityMapper.class);
        job.setReducerClass(RecordSimilarity.SimilarityReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("resi.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class SimilarityMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int bucketCount;
        private int hash;
        private int idOrdinal;
        private  int partitonOrdinal;
        private int hashPair;
        private int hashCode;
   	 	private boolean interSetMatching;
   	 	private  boolean  isBaseSetSplit;
   	 	private GenericAttributeSchema schema;
   	 	private static final int hashMultiplier = 1000;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	bucketCount = config.getInt("resi.bucket.count", 1000);
        	
        	//schema
        	String shemaPath = Utility.assertStringConfigParam(config, "resi.schema.path", "missing shema file path");
        	schema = BasicUtils.getGenericAttributeSchema(shemaPath);
        	
        	partitonOrdinal = schema.getPartitionField().getOrdinal();
        	idOrdinal = schema.getIdField().getOrdinal();
        	
        	//inter set matching
       	 	interSetMatching = config.getBoolean("resi.inter.set.matching",  false);
       	 	String baseSetSplitPrefix = config.get("resi.base.set.split.prefix", "base");
       	 	isBaseSetSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(baseSetSplitPrefix);
       	 	
       	
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            String partition = partitonOrdinal >= 0 ? items[partitonOrdinal] :  "N";
            hashCode = BasicUtils.positiveHashCode(items[idOrdinal]);

            if (interSetMatching) {
            	// 2 sets
	    		hash = hashCode %  bucketCount ;
            	if (isBaseSetSplit) {
    	    		for (int i = 0; i < bucketCount;  ++i) {
	       				hashPair = hash * hashMultiplier +  i;
	       				initKeyVal();
	       				outKey.add(partition, hashPair, 0);
	       				outVal.add(0 , value.toString());
		   	   			context.write(outKey, outVal);
    	    		}
            	} else {
    	    		for (int i = 0; i < bucketCount;  ++i) {
	    				hashPair =  i * hashMultiplier  +  hash;
	    				initKeyVal();
	       				outKey.add(partition, hashPair, 1);
	       				outVal.add(1 , value.toString());
		   	   			context.write(outKey, outVal);
    	    		}            		
            	}
            } else {
            	// 1 set
	    		hash = (hashCode %  bucketCount) / 2 ;
	    		for (int i = 0; i < bucketCount;  ++i) {
	    			initKeyVal();
	    			if (i < hash){
	       				hashPair = hash * hashMultiplier +  i;
	       				outKey.add(partition, hashPair, 0);
	       				outVal.add(0 , value.toString());
	       	   		 } else {
	    				hashPair =  i * hashMultiplier  +  hash;
	       				outKey.add(partition, hashPair, 1);
	       				outVal.add(1 , value.toString());
	    			} 
	   	   			context.write(outKey, outVal);
	    		}
	        }
            
        }  
        
        private void initKeyVal() {
			outKey.initialize();
			outVal.initialize();
        }
	}
	
	/**
	* @author pranab
  	*
  	*/
	public static class SimilarityReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
 		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private int outputPrecision;

		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	outputPrecision = config.getInt("cacd.output.precision", 3);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
				throws IOException, InterruptedException {
		}	
		
	}	
	
	/**
	 * @param args
	 */
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RecordSimilarity(), args);
		System.exit(exitCode);
	}
	
}
