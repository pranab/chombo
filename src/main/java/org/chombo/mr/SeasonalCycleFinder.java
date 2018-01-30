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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.stats.HistogramStat;
import org.chombo.util.BasicUtils;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Finds seasonal cycles by analyzing id distribution with cycles
 * @author pranab
 *
 */
public class SeasonalCycleFinder extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Seasonal cycle finder";
        job.setJobName(jobName);
        
        job.setJarByClass(SeasonalCycleFinder.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(SeasonalCycleFinder.FinderMapper.class);
        job.setReducerClass(SeasonalCycleFinder.FinderReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("scf.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class FinderMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] idOrdinals;
        private boolean entropyBasedFilter;
        private double maxEntropy;
        private int modeCount;
        private int minPercentAboveAverage;
		private HistogramStat histogram = new HistogramStat(1);
		private boolean countBasedModeSel;
		private int cycleIndx;
		private int cycleValue;
		private boolean toEmit;
		private int indx;
		private int numBins;
		private double entropy;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	idOrdinals = Utility.assertIntArrayConfigParam(config, "scf.id.field.ordinals", Utility.configDelim, 
        			"missing id ordinals");
        	entropyBasedFilter = config.getBoolean("scf.entropy.based.filter", false);
        	if (entropyBasedFilter) {
        		maxEntropy = Utility.assertDoubleConfigParam(config, "scf.max.entropy", "missing max entropy");
        	} 
        	
        	countBasedModeSel = config.getBoolean("scf.mode.count.based", true);
        	if (countBasedModeSel) {
        		modeCount = Utility.assertIntConfigParam(config, "scf.count.based.mode.sel", "missing top mode count");
        	} else {
        		minPercentAboveAverage = Utility.assertIntConfigParam(config, "scf.min.percent.above.average", 
        				"missing min percentage above average");
        	}
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            indx = idOrdinals.length;
            numBins = BasicUtils.extractIntFromStringArray(items, indx);
            ++indx;
            histogram.initialize();
            for (int i = indx; i < 2 * numBins; i += 2) {
            	cycleIndx =  BasicUtils.extractIntFromStringArray(items, i);
            	cycleValue = BasicUtils.extractIntFromStringArray(items, i + 1);
            	histogram.addBin(cycleIndx, cycleValue);
            }
            
            HistogramStat.Bin[] sortedBins = histogram.getSortedBinsByCount();
            toEmit = true;
            if (entropyBasedFilter) {
            	indx += 2 * numBins;
            	entropy = BasicUtils.extractDoubleFromStringArray(items, indx);
            	toEmit = entropy < maxEntropy;
            }     
            
            if (toEmit) {
	            if (countBasedModeSel) {
	            	//top n modes
	            	for (int i = sortedBins.length -1,  j = 0; i >= 0 && j < modeCount; --i, ++j){
	            		HistogramStat.Bin bin = sortedBins[i];
	            		emitOutput(bin, context);
	            	}
	            } else {
	            	//modes with value above threshold
	            	int thershold = (histogram.getMeanCount() * minPercentAboveAverage) / 100;
	            	for (int i = sortedBins.length -1;  i >= 0;  --i) {
	            		 if (sortedBins[i].getCount() > thershold) {
	 	            		HistogramStat.Bin bin = sortedBins[i];
		            		emitOutput(bin, context);
	            		 } else {
	            			 break;
	            		 }
	            	}
	            }
            }
        }
        
        /**
         * @param bin
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void emitOutput(HistogramStat.Bin bin, Context context) throws IOException, InterruptedException {
    		outKey.initialize();
    		outVal.initialize();
    		
    		cycleIndx = bin.getIndex();
    		cycleValue = bin.getCount();
    		outKey.add(cycleIndx);
    		outVal.addFromArray(items, idOrdinals);
    		outVal.add(cycleValue);
        	context.write(outKey, outVal);
        }
	}

    /**
    * @author pranab
    *
    */
    public static class  FinderReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private Text outVal = new Text();
 		private String fieldDelim;
		private boolean compactOutput;
		private StringBuilder stBld =  new StringBuilder();;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	compactOutput = config.getBoolean("scf.compact.output", true);
 		}

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	key.setDelim(fieldDelim);
			if (compactOutput) {
				stBld.delete(0, stBld.length());
				stBld.append(key.toString());
			}
    		for (Tuple val : values) {
    			val.setDelim(fieldDelim);
    			if (compactOutput) {
    				stBld.append(fieldDelim).append(val.toString());
    			} else {
    				outVal.set(key.toString() + fieldDelim + val.toString());
    				context.write(NullWritable.get(), outVal);
    			}
    		}
			if (compactOutput) {
				outVal.set(stBld.toString());
				context.write(NullWritable.get(), outVal);
			}    		
    		
        }
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SeasonalCycleFinder(), args);
		System.exit(exitCode);
	}
    
}
