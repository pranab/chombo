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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.chombo.stats.HistogramStat;
import org.chombo.stats.HistogramUtility;
import org.chombo.util.BasicUtils;
import org.chombo.util.GenericAttributeSchema;
import org.chombo.util.Pair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class NumericalAttrDistrStats extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Distribution based stats for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericalAttrDistrStats.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(NumericalAttrDistrStats.StatsMapper.class);
        job.setReducerClass(NumericalAttrDistrStats.StatsReducer.class);
        job.setCombinerClass(NumericalAttrDistrStats.StatsCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("nads.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class StatsMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private int conditionedAttr;
        private String[] items;
        private Map<Integer, Double> attrBinWidths;
        private double fieldVal;
        private int binIndex;
        private static final int ONE = 1;
        private GenericAttributeSchema schema;
        private int[] idOrdinals;
        private boolean needBinning;
        private int binOrdinal;
        private int binCount;
        private int binCountOrdinal;
               
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	needBinning = config.getBoolean("nads.need.binning", true);
        	attrBinWidths = Utility.assertIntegerDoubleMapConfigParam(config, "nads.attr.bucket.width.list", Utility.configDelim, 
        			Utility.configSubFieldDelim, "missing attrubutes ordinals and bucket widths");
        	conditionedAttr = config.getInt("nads.conditioned.attr",-1);
        	idOrdinals = Utility.optionalIntArrayConfigParam(config, "nads.id.field.ordinals", Utility.configDelim);
        	if (!needBinning) {
        		binOrdinal = Utility.assertIntConfigParam(config, "nads.bin.ordinal", "missing bin ordinal");
        		binCountOrdinal = Utility.assertIntConfigParam(config, "nads.bin.count.ordinal", "missing bin ordinal");
        	}
        	
        	//validate attributes
        	schema = Utility.getGenericAttributeSchema(config, "nads.schema.file.path");
        	if (null != schema) {
        		Set<Integer> attrSet = attrBinWidths.keySet();
        		int[] attrs = new int[attrSet.size()];
        		int i = 0;
        		for (Integer attr : attrSet) {
        			attrs[i++] = attr;
        		}
        		if (!schema.areNumericalAttributes(attrs)) {
        			throw new IllegalArgumentException("attributes must be numerical");
        		}
        	}
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
        	for (int attr : attrBinWidths.keySet()) {
            	outKey.initialize();
            	outVal.initialize();
            	
            	if (null != idOrdinals) {
            		outKey.addFromArray(items, idOrdinals);
            	}
            	
            	if (needBinning) {
            		outKey.add(attr);
                	if (conditionedAttr >= 0) {
                		//conditioned on another attribute
                		outKey.add(items[conditionedAttr]);
                	}   
                	
            		//data needs binning
	            	fieldVal = Double.parseDouble(items[attr]);
	            	binIndex = (int)(fieldVal / attrBinWidths.get(attr));
	            	outVal.add(binIndex, ONE);
            	} else {
            		//data already has bin index and count
            		binIndex = Integer.parseInt(items[binOrdinal]);
            		binCount = Integer.parseInt(items[binCountOrdinal]);
	            	outVal.add(binIndex, binCount);
            	}
            	context.write(outKey, outVal);
        	}
        }
 	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class StatsCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private Map<Integer, Integer> attrBinCounts = new HashMap<Integer, Integer>();
        private int binIndex;
        private int count;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	attrBinCounts.clear();
    		for (Tuple val : values) {
    			for (int i = 0; i < val.getSize(); i += 2) {
    				binIndex = val.getInt(i);
        			count = val.getInt(i + 1);
        			Integer curCount = attrBinCounts.get(binIndex);
    				if (null == curCount) {
    					curCount = count;
    				} else {
    					curCount += count;
    				}
    				attrBinCounts.put(binIndex, curCount);
    			}
    		}
	    	outVal.initialize();
	    	for (int binIndex :  attrBinCounts.keySet()) {
	        	outVal.add(binIndex,  attrBinCounts.get(binIndex));
	    	}
	        context.write(key, outVal);       	
        }		
	}	
	
	/**
	* @author pranab
  	*
  	*/
	public static class StatsReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private HistogramStat histogram = new HistogramStat();
        private int binIndex;
        private int count;
        private Map<Integer, Double> attrBinWidths = new HashMap<Integer, Double>();
    	private int conditionedAttr;
        private GenericAttributeSchema schema;
        private boolean outputNormalizedHist;
        private int outputPrecision;
        private int[] idOrdinals;
        private boolean needBinning;
        private int attr;
        private int attrIndex;
        private String fieldDelimIn;
        private Map<Tuple, HistogramStat> refDistr = new HashMap<Tuple, HistogramStat>();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	fieldDelimIn = config.get("field.delim.regex", ",");
			
        	attrBinWidths = Utility.assertIntegerDoubleMapConfigParam(config, "nads.attr.bucket.width.list", Utility.configDelim, 
        			Utility.configSubFieldDelim, "missing attrubutes ordinals and bucket widths");
        	conditionedAttr = config.getInt("nads.conditioned.attr",-1);
        	needBinning = config.getBoolean("nads.need.binning", true);
        	idOrdinals = Utility.optionalIntArrayConfigParam(config, "nads.id.field.ordinals", Utility.configDelim);

        	//validation with schema
           	schema = Utility.getGenericAttributeSchema(config, "nads.schema.file.path");
            if (null != schema) {
            	
            }
            
            outputPrecision = config.getInt("nads.output.precision", 3);
            histogram.withOutputPrecision(outputPrecision).withFieldDelim(fieldDelim);
            outputNormalizedHist = config.getBoolean("nads.output.normalized.hist", true);
            
            //reference distr for KL divergence
            buildRefDistr(config);
            
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			histogram.initialize();
			attr = 0;
			if (needBinning) {
				attrIndex = null != idOrdinals ? idOrdinals.length  :  0;
				attr = key.getInt(attrIndex);
			}
			double binWidth = attrBinWidths.get(attr);
			histogram.setBinWidth(binWidth);
			for (Tuple val : values) {
				for (int i = 0; i < val.getSize(); i += 2) {
	    			binIndex = val.getInt(i);
	    			count = val.getInt(i+1);
	    			histogram.addBin(binIndex, count);
				}
			}
			emitOutput( key,  context);
		}
 	
		/**
		 * @param config
		 * @throws IOException
		 */
		private void buildRefDistr(Configuration config) throws IOException {
			InputStream inStr =  Utility.getFileStream(config, "nads.ref.distr.file.path");
			if (null != inStr) {
				int keyLen = null != idOrdinals ? idOrdinals.length : 0;
				++keyLen;
				Map<String[], HistogramStat> distrMap = HistogramUtility.createHiostograms(inStr, keyLen, true);
				for (String[] key : distrMap.keySet()) {
					HistogramStat hist = distrMap.get(key);
					Tuple keyTuple = new Tuple();
					for (int i = 0; i < keyLen - 1; ++i) {
						keyTuple.add(key[i]);
					}
					keyTuple.add(Integer.parseInt(key[keyLen - 1]));
					refDistr.put(keyTuple, hist);
				}
			}
		}
		 
		/**
		 * @param key
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected  void emitOutput(Tuple key,  Context context) throws IOException, InterruptedException {
			stBld.delete(0, stBld.length());
			if (!fieldDelim.equals(",")) {
				key.setDelim(fieldDelim);
			}
			stBld.append(key.toString()).append(fieldDelim);
			if (outputNormalizedHist) {
				//normalized
				stBld.append(histogram.normalizedBinsToString());
			} else {
				//raw
				stBld.append(histogram.binsToString());
			}
			stBld.append(fieldDelim).append(BasicUtils.formatDouble(histogram.getEntropy(), outputPrecision)) ;
			stBld.append(fieldDelim).append(BasicUtils.formatDouble(histogram.getMode(), outputPrecision)) ;
			stBld.append(fieldDelim).append(BasicUtils.formatDouble(histogram.getQuantile(0.25), outputPrecision)) ;
			stBld.append(fieldDelim).append(BasicUtils.formatDouble(histogram.getQuantile(0.50), outputPrecision)) ;
			stBld.append(fieldDelim).append(BasicUtils.formatDouble(histogram.getQuantile(0.75), outputPrecision)) ;
			
			//KL divergence 
			if (!refDistr.isEmpty()) {
				HistogramStat refHist = refDistr.get(key);
				if (null == refHist) {
					throw new IllegalStateException("missing referenec distribution for key " + key);
				}
				Pair<Double, Integer> diverge = HistogramUtility.findKullbackLeiblerDivergence(refHist, histogram);
				stBld.append(fieldDelim).append(BasicUtils.formatDouble(diverge.getLeft(), outputPrecision)) ;
				stBld.append(fieldDelim).append(diverge.getRight());
			}
			
			outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericalAttrDistrStats(), args);
        System.exit(exitCode);
	}

}
