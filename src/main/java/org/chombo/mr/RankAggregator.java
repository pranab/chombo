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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Aggregates rank of an item from multiple list according to configured aggregation strategy
 * @author pranab
 *
 */
public class RankAggregator extends Configured implements Tool {
    private static final String AGGR_AVG = "average";
    private static final String AGGR_WTD_AVG = "weightedAverage";
    private static final String AGGR_MED = "median";
    private static final String AGGR_MAX = "max";
    private static final String AGGR_PROD = "product";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Aggregates rank from multiple rank lists";
        job.setJobName(jobName);
        
        job.setJarByClass(RankAggregator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(RankAggregator.AggregateMapper.class);
        job.setReducerClass(RankAggregator.AggregationReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("raa.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class AggregateMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] idOrdinals;
        private int rankOrdinal;
        private double listWeight;
        private Map<String, Double> listWeights;
        private String aggregationStrategy;
        private static final int  SPLIT_PREFIX_LEN = 4;
   
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	idOrdinals = Utility.assertIntArrayConfigParam(config, "raa.id.field.ordinals", Utility.configDelim, "missing id field ordinals");
           	rankOrdinal = Utility.assertIntConfigParam(config, "raa.rank.attr.ordinal", "missing rank attribute ordinal");
           	
        	aggregationStrategy = config.get("raa.rank.agg.strategy", AGGR_PROD);
        	if (aggregationStrategy.equals(AGGR_WTD_AVG)) {
        		//map 4 alpha numeric split prefix and weight
        		listWeights = Utility.assertStringDoubleMapConfigParam(config, "raa.list.weightl", 
           			Utility.configDelim, Utility.configSubFieldDelim, "missing split prefix weight");
        		String splitPrefix = ((FileSplit)context.getInputSplit()).getPath().getName().substring(0, SPLIT_PREFIX_LEN);
        		listWeight = listWeights.get(splitPrefix);
        	}
        }        
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            
            Utility.createStringTuple(items, idOrdinals, outKey);
            outVal.initialize();
            outVal.add(Double.parseDouble(items[rankOrdinal]));
            if (aggregationStrategy.equals(AGGR_WTD_AVG)) {
            	outVal.add(listWeight);
            }
        	context.write(outKey, outVal);
        }
	}

	   /**
     * @author pranab
     *
     */
    public static class AggregationReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private Text outVal = new Text();
    	private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
        private int outputPrecision ;
        private String aggregationStrategy;
        private List<Double> ranks = new ArrayList<Double>();
        private List<Double> weights = new ArrayList<Double>();
        private double aggrRank;
        private double aggr;
        private double weightSum;
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	aggregationStrategy = config.get("raa.rank.agg.strategy", AGGR_PROD);
        	outputPrecision = config.getInt("raa.output.prec", 3);
		}
		
	   	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		key.setDelim(fieldDelim);
    		ranks.clear();
    		weights.clear();
    		stBld.delete(0, stBld.length());

    		for (Tuple val : values) {
    			ranks.add(val.getDouble(0));
    			if (aggregationStrategy.equals(AGGR_WTD_AVG)) {
    				weights.add(val.getDouble(1));
    			}
    		}
    		
    		if (ranks.size() == 1) {
    			aggrRank = ranks.get(0);
    		} else {
	    		if (aggregationStrategy.equals(AGGR_AVG)) {
	    			//average
	    			aggr = 0;
	    			for (double rank : ranks) {
	    				aggr += rank;
	    			}
	    			aggrRank = aggr / ranks.size();
	    		} if (aggregationStrategy.equals(AGGR_WTD_AVG)) {
	    			//weighted average
	    			aggr = 0;
	    			weightSum = 0;
	    			for (int i = 0; i < ranks.size(); ++i) {
	    				aggr += ranks.get(i) * weights.get(i);
	    				weightSum += weights.get(i); 
	    			}
	    			aggrRank = aggr / weightSum;
	    		} else if (aggregationStrategy.equals(AGGR_MED)) {
	    			//median
	    			Collections.sort(ranks);
	    			int mid = ranks.size() / 2;
	    			if (ranks.size() % 2 == 1) {
	    				aggrRank = ranks.get(mid);
	    			} else {
	    				aggrRank = (ranks.get(mid -1) + ranks.get(mid)) / 2 ;
	    			}
	    		} else if (aggregationStrategy.equals(AGGR_MAX)) {
	    			//max
	    			Collections.sort(ranks);
	    			aggrRank = ranks.get(ranks.size() - 1);
	    		} else if (aggregationStrategy.equals(AGGR_PROD)) {
	    			//product
	    			aggr = 1.0;
	    			for (double rank : ranks) {
	    				aggr *= rank;
	    			}
	    			aggrRank = Math.pow(aggr, 1.0 / ranks.size() );
	    		} else {
	    			throw new IllegalArgumentException("invalid rank aggregation strategy");
	    		}
    		}
    		
    		stBld.append(key.toString()).append(fieldDelim).append(Utility.formatDouble(aggrRank, outputPrecision));
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}		
    }	
    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RankAggregator(), args);
        System.exit(exitCode);
	}
    
}
