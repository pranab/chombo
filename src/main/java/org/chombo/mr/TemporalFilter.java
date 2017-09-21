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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Pair;
import org.chombo.util.SeasonalAnalyzer;
import org.chombo.util.Utility;

/**
 * Filters tempooral data according seasonality criteria defined
 * @author pranab
 *
 */
public class TemporalFilter   extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Temporal filtering based seasonal parameters";
        job.setJobName(jobName);
        
        job.setJarByClass(TemporalFilter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(TemporalFilter.FilterMapper.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private String fieldDelimRegex;
        private String[] items;
        private int timeStampFieldOrdinal;
        private String seasonalCycleType;
        private int cycleIndex;
        private long timeStamp;
        private SeasonalAnalyzer seasonalAnalyzer;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	timeStampFieldOrdinal = Utility.assertIntConfigParam(config,"tef.time.stamp.field.ordinal", "missing timestamp field ordinal");
        	seasonalCycleType = Utility.assertStringConfigParam(config,"tef.seasonal.cycle.type", "missing seasonal cycle type");
        	
    		seasonalAnalyzer = new SeasonalAnalyzer(seasonalCycleType);
    		
    		//additional configuration
        	if (seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_DAY ) ||  
        			seasonalCycleType.equals(SeasonalAnalyzer.HOUR_RANGE_OF_WEEK_END_DAY ) ) {
        		Map<Integer, Integer>  hourRanges = Utility. assertIntIntegerIntegerMapConfigParam(config, "tef.hour.groups", 
        				Utility.configDelim, Utility.configSubFieldDelim, "missing hour groups");
        		seasonalAnalyzer.setHourRanges(hourRanges);
        	} else if (seasonalCycleType.equals(SeasonalAnalyzer.ANY_TIME_RANGE)) {
        		List<Pair<Integer, Integer>>timeRanges =  Utility. assertIntPairListConfigParam(config, "tef.time.range",  
        				Utility.configDelim, Utility.configSubFieldDelim, "missing time range");
        		seasonalAnalyzer.setTimeRanges(timeRanges);
        	}
        	
        	//time zone adjustment
        	int  timeZoneShiftHours = config.getInt("tef.time.zone.shift.hours",  0);
        	if (timeZoneShiftHours > 0) {
        		seasonalAnalyzer.setTimeZoneShiftHours(timeZoneShiftHours);
        	}
        	
        	//timestamp unit
        	boolean timeStampInMili = config.getBoolean("tef.time.stamp.in.mili", true);
        	seasonalAnalyzer.setTimeStampInMili(timeStampInMili);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex, -1);
            timeStamp = Long.parseLong(items[timeStampFieldOrdinal]);
            cycleIndex = seasonalAnalyzer.getCycleIndex(timeStamp);
            
            if (cycleIndex >= 0) {
            	context.write(NullWritable.get(), value);
            }
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TemporalFilter(), args);
		System.exit(exitCode);
	}

	
}
