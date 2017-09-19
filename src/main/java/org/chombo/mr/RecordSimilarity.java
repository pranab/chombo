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
import org.chombo.distance.AttributeDistanceSchema;
import org.chombo.distance.InterRecordDistance;
import org.chombo.util.Attribute;
import org.chombo.util.BasicUtils;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class RecordSimilarity extends Configured implements Tool {
	private static final int hashMultiplier = 1000;

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
        
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

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
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	bucketCount = config.getInt("resi.bucket.count", 1000);
        	
        	//schema
        	String shemaPath = Utility.assertStringConfigParam(config, "resi.schema.path", "missing shema file path");
        	InputStream inStr = Utility.getFileStream(shemaPath);
        	schema = BasicUtils.getGenericAttributeSchema(inStr);
        	
        	//partiton and ID  column ordinal
        	Attribute partField =  schema.getPartitionField();
        	partitonOrdinal = null  !=  partField ? partField.getOrdinal() : -1;
        	idOrdinal = schema.getIdField().getOrdinal();
        	
        	//inter set matching
       	 	interSetMatching = config.getBoolean("resi.inter.set.matching",  false);
       	 	if (interSetMatching) {
       	 		//String baseSetSplitPrefix = config.get("resi.base.set.split.prefix", "base");
       	 		String baseSetSplitPrefix = Utility.assertStringConfigParam(config, "resi.base.set.split.prefix", 
       	 				"missing base split prefix");
       	 		isBaseSetSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(baseSetSplitPrefix);
       	 	}
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
	    		hash = (hashCode %  bucketCount)  ;
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
        
        /**
         * 
         */
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
		private InterRecordDistance recDistance;
        private int idOrdinal;
        private String fieldDelimRegex;
        private String subFieldDelim;
        private int distThreshold;
        private List<String> valueList = new ArrayList<String>();
        private boolean inFirstBucket;
        private int firstBucketSize;
        private int secondBucketSize;
        private String firstId;
        private String  secondId;
        private int dist;
        private boolean outputRecord;
        private boolean outputIdFirst;

		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
			fieldDelim = config.get("field.delim.out", ",");
        	outputPrecision = config.getInt("resi.output.precision", 3);
        	
        	//schema
        	String shemaPath = Utility.assertStringConfigParam(config, "resi.schema.path", "missing shema file path");
        	InputStream inStr = Utility.getFileStream(shemaPath);
        	GenericAttributeSchema schema = BasicUtils.getGenericAttributeSchema(inStr);
        	
        	//distance calculation related schema
        	String distSchemaPath = Utility.assertStringConfigParam(config, "resi.dist.schema.path", "missing distance shema file path");
        	InputStream inStrDist = Utility.getFileStream(distSchemaPath);
        	AttributeDistanceSchema distSchema = BasicUtils.getDistanceSchema(inStrDist);

        	//inter record distance finder
        	recDistance = new InterRecordDistance(schema,distSchema,fieldDelim);
        	
        	//id
            idOrdinal = schema.getIdField().getOrdinal();
            
            //scale
        	int scale = config.getInt("resi.distance.scale", 1000);
        	recDistance.withScale(scale);
        	
        	//faceted fields
        	int[] facetedFields = Utility.intArrayFromString(config, "resi.faceted.field.ordinal", Utility.configDelim);
        	if (null != facetedFields) {
        		recDistance.withFacetedFields(facetedFields);
        	}

        	//output ID first
        	outputIdFirst =   config.getBoolean("resi.output.id.first", true);      	
        	
        	//double range
        	boolean doubleRange = config.getBoolean("resi.double.range", false);
        	recDistance.withDoubleRange(doubleRange);

        	//categorical set
        	boolean categoricalSet = config.getBoolean("resi.categorical.set", false);
        	recDistance.withCategoricalSet(categoricalSet);
        	
        	//sub fields
        	subFieldDelim = config.get("resi.sub.field.delim.regex", "::");
        	
        	//distance threshold for output
        	distThreshold = config.getInt("resi.dist.threshold", scale);
        	
        	//output whole record
        	outputRecord =  config.getBoolean("resi.output.record", false);     
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
				throws IOException, InterruptedException {
        	valueList.clear();
        	inFirstBucket = true;
        	firstBucketSize = secondBucketSize = 0;
        	int secondPart = key.getInt(1);
        	//System.out.println("hash pair:" + secondPart);
        	
        	if (secondPart / hashMultiplier == secondPart % hashMultiplier){
        		//same hash bucket
	        	for (Tuple value : values){
	        		valueList.add(value.getString(1));
	        	}
	        	firstBucketSize = secondBucketSize = valueList.size();
	        	for (int i = 0;  i < valueList.size();  ++i){
	        		String first = valueList.get(i);
	        		firstId =  first.split(fieldDelimRegex, -1)[idOrdinal];
	        		for (int j = i+1;  j < valueList.size();  ++j) {
	            		String second = valueList.get(j);
	            		secondId =  second.split(fieldDelimRegex, -1)[idOrdinal];
	            		if (!firstId.equals(secondId)){
		        			dist  = recDistance.findScaledDistance(first, second);
		        			if (dist <= distThreshold) {
		        				outVal.set(createValueField(first, second));
		        				context.write(NullWritable.get(), outVal);
		        			}
	            		} 
	   				}
	        	}
	           //	System.out.println("same bucket size:" + firstBucketSize );
        	} else {
        		//different hash bucket
	        	for (Tuple value : values){
	        		if (value.getInt(0) == 0) {
	        			valueList.add(value.getString(1));
	        		} else {
	        			if (inFirstBucket) {
	        				firstBucketSize = valueList.size();
	        				inFirstBucket = false;
	        			}
	        			++secondBucketSize;
	        			String second = value.getString(1);
	            		secondId =  second.split(fieldDelimRegex, -1)[idOrdinal];
	            		for (String first : valueList){
	                		firstId =  first.split(fieldDelimRegex, -1)[idOrdinal];
		        			dist  = recDistance.findScaledDistance(first, second);
		        			if (dist <= distThreshold) {
		        				outVal.set(createValueField(first, second));
		        				context.write(NullWritable.get(), outVal);
		        			}
	            		}
	        		}
	        	}
            	//System.out.println("firstBucketSize:" + firstBucketSize + " secondBucketSize:" + secondBucketSize );
        	}
        	
		}	
		
        /**
         * generates output to emit
         * @return
         */
        private String createValueField(String first, String second) {
        	stBld.delete(0, stBld.length());
        	if (outputIdFirst) {
        		stBld.append(firstId).append(fieldDelim).append(secondId).append(fieldDelim);
        	}
        	if (outputRecord) {
        		stBld.append(first).append(fieldDelim);
        		stBld.append(second).append(fieldDelim);
        	}
        	stBld.append(dist);
        	return stBld.toString();
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
