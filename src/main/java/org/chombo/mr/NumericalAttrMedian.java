package org.chombo.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.RichAttribute;
import org.chombo.util.RichAttributeSchema;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class NumericalAttrMedian extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Basic stats for numerical attributes";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericalAttrMedian.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(NumericalAttrMedian.StatsMapper.class);
        job.setReducerClass(NumericalAttrMedian.StatsReducer.class);

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
	public static class StatsMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
		private int[]  attributes;
        private String fieldDelimRegex;
        private String[] items;
        private RichAttributeSchema schema;
        private RichAttribute[] numericAttrs;
        private double val;
        private int bin;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	schema = Utility.getRichAttributeSchema(config, "rich.attribute.schema");
        	attributes = Utility.intArrayFromString(config.get("attr.list"), fieldDelimRegex);
        	if (null == attributes) {
        		//all numeric fields
        		attributes = schema.getNumericAttributeOrdinals();
        	}
        	
        	//get all meta data
        	numericAttrs = new RichAttribute[attributes.length];
        	for (int i = 0; i < attributes.length; ++i) {
        		numericAttrs[i] = schema.findAttributeByOrdinal(attributes[i]);
        	}
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
        	for (int i = 0; i < attributes.length; ++i) {
            	outKey.initialize();
            	outVal.initialize();
            	val = Double.parseDouble(items[attributes[i]]);
            	bin = (int)(val / numericAttrs[i].getBucketWidth());
            	outKey.add(attributes[i], bin);
            	
            	outVal.add(bin, val);
            	context.write(outKey, outVal);
        	}
        }
	}
	
	/**
	* @author pranab
  	*
  	*/
	public static class StatsReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		protected int totalCount;
		private int count;
		private int bin;
		private Map<Integer, List<Double>> histogram = new TreeMap<Integer, List<Double>>();
		private double median;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			totalCount = 0;
			histogram.clear();
			for(Tuple value : values) {
				bin = value.getInt(0);
				List<Double> fieldValues = histogram.get(bin);
				if (null == fieldValues) {
					fieldValues = new ArrayList<Double>();
					histogram.put(bin, fieldValues);
				} 
				fieldValues.add(value.getDouble(1));
				++totalCount;
			}
 		
			//find median
			int midPoint = totalCount / 2;
			count = 0;
			for (int i : histogram.keySet()) {
				List<Double> fieldValues = histogram.get(i);
				if (count + fieldValues.size() > midPoint) {
					int offset = midPoint - count;
					Collections.sort(fieldValues);
					median = fieldValues.get(offset);
					if (midPoint % 2 == 0) {
						//take average of adjacent points
						if (offset > 0) {
							median = (median + fieldValues.get(offset -1)) / 2;
						} else {
							//last element from previous bin
							List<Double> prevBinfieldValues = histogram.get(i-1);
							Collections.sort(prevBinfieldValues);
							median = (median + prevBinfieldValues.get(prevBinfieldValues.size() - 1)) / 2;
						}
					}
					break;
				} else {
					count += histogram.get(i).size();
				}
			}
			
			outVal.set("" + key.getInt(0) + fieldDelim + median);
			context.write(NullWritable.get(), outVal);
		}
	}	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NumericalAttrMedian(), args);
        System.exit(exitCode);
	}


}
