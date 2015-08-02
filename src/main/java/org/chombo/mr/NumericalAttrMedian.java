package org.chombo.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
 * Calulates median and median absolute difference
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
        private String operation;
        private Map<Integer, Double> medians = new HashMap<Integer, Double>();
        private int[] idOrdinals;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
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
        	
        	operation = config.get("op.type", "med");
        	if (operation.equals("mad")) {
        		//median of deviation from median
        		List<String> lines = Utility.getFileLines(config, "med.file.path");
        		for (String line : lines) {
        			String[] items = line.split(fieldDelimRegex);
        			medians.put(Integer.parseInt(items[0]), Double.parseDouble(items[1]));
        		}
        	}
        	
        	idOrdinals = Utility.intArrayFromString(config.get("id.ordinals"), fieldDelimRegex);
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            
        	for (int i = 0; i < attributes.length; ++i) {
            	outKey.initialize();
            	outVal.initialize();
            	val = Double.parseDouble(items[attributes[i]]);
            	if (operation.equals("mad")) {
            		val = Math.abs(val - medians.get(attributes[i]));
            	}
            	bin = (int)(val / numericAttrs[i].getBucketWidth());
            	
            	if (null != idOrdinals) {
            		//record id avalable
            		for (int ord  :  idOrdinals) {
            			outKey.add(items[ord]);
            		}
            	}
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
		private double med;
		private double mad;
        private String operation;
        private int[] idOrdinals;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	operation = config.get("op.type", "med");
        	idOrdinals = Utility.intArrayFromString(config.get("id.ordinals"), fieldDelim);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
     	throws IOException, InterruptedException {
			totalCount = 0;
			histogram.clear();
			stBld.delete(0, stBld.length());
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
					//found the bin that has the median
					int offset = midPoint - count;
					Collections.sort(fieldValues);
					med = fieldValues.get(offset);
					if (midPoint % 2 == 0) {
						//take average of adjacent points
						if (offset > 0) {
							//adjacent points in the same bin
							med = (med + fieldValues.get(offset -1)) / 2;
						} else {
							//last element from previous bin
							List<Double> prevBinfieldValues = histogram.get(i-1);
							Collections.sort(prevBinfieldValues);
							med = (med + prevBinfieldValues.get(prevBinfieldValues.size() - 1)) / 2;
						}
					}
					break;
				} else {
					//keep going
					count += histogram.get(i).size();
				}
			}
			
        	if (null != idOrdinals) {
        		//record id available
        		for (int i = 0;  i < idOrdinals.length; ++i) {
        			stBld.append(key.getString(i)).append(fieldDelim);
        		}
    			stBld.append(key.getInt(idOrdinals.length)).append(fieldDelim);
        	} else {
        		stBld.append(key.getInt(0)).append(fieldDelim);
			}
        	
        	if (operation.equals("mad")) {
        		mad = 1.4296 * med;
        		stBld.append(mad);
        	} else {
        		stBld.append(med);
        	}
        	
        	outVal.set(stBld.toString());
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
