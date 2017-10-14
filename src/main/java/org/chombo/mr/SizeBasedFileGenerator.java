
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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Consolidates and splits files based on size or line count
 * @author pranab
 *
 */
public class SizeBasedFileGenerator extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Consolidates and splits files";
        job.setJobName(jobName);
        
        job.setJarByClass(SizeBasedFileGenerator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(SizeBasedFileGenerator.GeneratorMapper.class);
        job.setReducerClass(SizeBasedFileGenerator.GeneratorReducer.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);

        int numReducer = job.getConfiguration().getInt("nam.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class GeneratorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String splitName;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	splitName = ((FileSplit)context.getInputSplit()).getPath().getName();
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	outKey.initialize();
        	outKey.add(splitName, key.get());
        	
        	outVal.initialize();
        	outVal.add(key.get(), value.toString());
        	context.write(outKey, outVal);
        }        
	}
	
	/**
	* @author pranab
  	*
  	*/
	public static class GeneratorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private long maxFileSize;
		private int maxFileLines;
		private String filePrefix;
		private Map<String, List<String>> fileMaps = new TreeMap<String, List<String>>();
		private int outputFileSeq = 1;
		private OutputStream outStr;
		private PrintWriter writer;
		private String outputDirePath;
		private int lineCount;
		private boolean splitByByteSize;
		private long lastOffset;
		private long size; 
		private long currentSize; 
		private String curOutputFile;
		
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
			splitByByteSize = config.getBoolean("sbfg.split.by.byte.size", true);
			if (splitByByteSize) {
				int maxMb = Utility.assertIntConfigParam(config, "sbfg.max.size.mb", "missing maximum file size");
				maxFileSize = maxMb * 1028L * 1028L;
			} else {
				maxFileLines = Utility.assertIntConfigParam(config, "sbfg.max.file.lines", "missing maximum file line count");
			}
			outputDirePath = Utility.assertStringConfigParam(config, "sbfg.output.dir.path", "missing output directory path");
			filePrefix = BasicUtils.generateId();
			
			openFileWriter();
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			closeFileWriter();
			
			//output input file mapping
			for (String outFile : fileMaps.keySet()) {
				List<String> inFiles = fileMaps.get(outFile);
				for (String inFile : inFiles) {
					outVal.set(outFile + fieldDelim + inFile);
					context.write(NullWritable.get(), outVal);
				}
			}
        }
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
			fileMaps.get(curOutputFile).add(key.getString(0));
			
			for (Tuple value : values) {
				writer.write(value.getString(1) + "\n");
				boolean startNewFile = false;
				if (splitByByteSize) {
					size = currentSize + value.getLong(0) - lastOffset;
					if (size >= maxFileSize) {
						startNewFile = true;
					}
				} else {
					if (++lineCount == maxFileLines) {
						startNewFile = true;
					}
				}
				
				//start new file
				if (startNewFile) {
					closeFileWriter();
					openFileWriter();
					if (splitByByteSize) {
						lastOffset = key.getLong(1);
						currentSize = 0;
						size = 0;
					} else {
						lineCount = 0;
					}
				}
			}
			if (splitByByteSize) {
				currentSize = size;
				lastOffset = 0;
			}
		}
		
		/**
		 * @throws IOException
		 */
		private void openFileWriter() throws IOException {
			++outputFileSeq;
			curOutputFile = filePrefix + "-" + BasicUtils.formatInt(outputFileSeq, 4);
			String filePath = outputDirePath +"/" + curOutputFile;
			outStr = Utility.getCreateFileOutputStream(filePath);
			writer = new PrintWriter(outStr);
			
			fileMaps.put(curOutputFile, new ArrayList<String>());
		}
		
		/**
		 * @throws IOException
		 */
		private void closeFileWriter() throws IOException {
			writer.close();
			outStr.close();
		}
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SizeBasedFileGenerator(), args);
        System.exit(exitCode);
	}
	
}
