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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Filters partitioned time sequence data. Filtering is on time field generating
 * a sub sequence
 * @author pranab
 *
 */
public class TimeSequenceFilter extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Time sequence to time gap sequence conversion";
        job.setJobName(jobName);
        
        job.setJarByClass(TimeSequenceFilter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(TimeGapSequenceGenerator.TimeGapMapper.class);
        job.setReducerClass(TimeSequenceFilter.FilterReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("tsf.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	* @author pranab
	*
	*/
	public static class  FilterReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private int numIDFields;
		private int numAttributes;
		private String filterType;
        private Long minDateTime;
        private Long maxDateTime;
        private boolean toEmit;
        private long timeStamp;
        private boolean includeRawDateTimeField;
        private boolean outputCompact;
        private List<String> compactRec = new ArrayList<String>();

		private static final String FIlT_EARLIEST = "earliest";
		private static final String FIlT_LATEST = "latest";
		private static final String FIlT_RANGE = "range";
		private static final String FIlT_NONE = "none";
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	filterType = config.get("tsf.filter.type");

        	if (filterType.equals(FIlT_RANGE)) {
                String dateFormatStr = config.get("tsf.date.format.str");
                SimpleDateFormat dateFormat = dateFormatStr != null ? new SimpleDateFormat(dateFormatStr) : null;
	            try {
		            String minDateTimeStr = config.get("tsf.min.date.time");
	            	minDateTime = getEpochTime(minDateTimeStr, dateFormat);
            
	            	String maxDateTimeStr = config.get("tsf.max.date.time");
	            	maxDateTime = getEpochTime(maxDateTimeStr, dateFormat);
	            	
	            	if (null == minDateTimeStr && null == maxDateTime) {
	            		throw new IllegalStateException("date time range not specified");
	            	}
	            } catch (ParseException ex) {
					throw new IOException("Failed to parse date time", ex);
	            }
        	}
        	numIDFields = Utility.intArrayFromString(config.get("tsf.id.field.ordinals"), configDelim).length;
        	numAttributes = Utility.assertIntArrayConfigParam(config, "tsf.quant.attr.list", configDelim, 
        			"missing quant attribute list").length;
        	includeRawDateTimeField = config.getBoolean("tsf.include.raw.date.time.field", false);
        	
        	//whole sequence in one record
        	outputCompact = config.getBoolean("tsf.output.compact", false);
		}

		/**
		 * @param dateTimeStr
		 * @param dateFormat
		 * @return
		 * @throws ParseException
		 */
		private Long getEpochTime(String dateTimeStr, SimpleDateFormat dateFormat) throws ParseException {
			Long epochTime = null;
            if (null != dateTimeStr) {
            	if (null != dateFormat) {
            		epochTime = dateFormat.parse(dateTimeStr).getTime();
            	} else {
            		epochTime = Long.parseLong(dateTimeStr);
            	}
            }
			return epochTime;
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		boolean first = true;
    		Tuple latest = null;
    		
    		if (outputCompact) {
        		stBld.delete(0, stBld.length());
        		addKey(key);
    		}
    		
    		for (Tuple val : values) {
    			if (filterType.equals(FIlT_EARLIEST) && first) {
    				//only the first in sequence
    				if (outputCompact) {
    					addValue(val);
    				} else {
    					emit(key, val, context);
    				}
    				break;
    			} else if (filterType.equals(FIlT_LATEST)) {
    				//only the last in sequence
    				latest = val;
    			} else if (filterType.equals(FIlT_RANGE)) {
    				//sequence within time window
    				timeStamp = val.getLong(0);
    				toEmit = (null == minDateTime || timeStamp > minDateTime) && 
    						(null == maxDateTime || timeStamp < maxDateTime);
    				if (toEmit) {
    					if (outputCompact) {
        					addValue(val);
    					} else {
    						emit(key, val, context);
    					}
    				}
    			} else if (filterType.equals(FIlT_NONE)) { 
    				//all, no filter
    				if (outputCompact) { 
    					addValue(val);
    				} else {
    					emit(key, val, context);
    				}
    			} else {
    				throw new IllegalStateException("invalid fliter type");
    			}
    		}
    		
    		if (null != latest) {
    			if (outputCompact) {
					addValue(latest);
    			} else {
    				emit(key, latest, context);
    			}
    		}
    		
    		if (outputCompact) {
    			doEmit(context);
    		}
        }
        
        /**
         * @param key
         * @param val
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void emit(Tuple  key, Tuple val, Context context) throws IOException, InterruptedException {
    		stBld.delete(0, stBld.length());
    		addKey(key);
    		addValue(val);
    		doEmit(context);
        }
        
        /**
         * @param key
         */
        private void addKey(Tuple  key) {
    		for (int i = 0; i < numIDFields; ++i) {
    			stBld.append(key.getString(i)).append(fieldDelim);
    		}
        }
        
        /**
         * @param val
         */
        private void addValue(Tuple  val) {
    		int offset = 1;
    		if (includeRawDateTimeField) {
    			stBld.append(val.getString(offset)).append(fieldDelim);
    			++offset;
    		}
    		for (int i = 0; i < numAttributes; ++i) {
    			stBld.append(val.getString(i + offset)).append(fieldDelim);
    		}
        }
        
        /**
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void doEmit(Context context) throws IOException, InterruptedException {
			stBld.deleteCharAt(stBld.length() -1);
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
        }       
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TimeSequenceFilter(), args);
		System.exit(exitCode);
	}
	

}
