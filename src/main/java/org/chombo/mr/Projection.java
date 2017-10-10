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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
import org.chombo.redis.RedisCache;
import org.chombo.util.AttributeFilter;
import org.chombo.util.BaseAttributeFilter;
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
import org.chombo.util.RowColumnFilter;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Simple query with projection. Can do group by and order by.  If grouped by can do count or 
 * unique count. sum and avearge
 * @author pranab
 *
 */
public class Projection extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Projection  and grouping  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(Projection.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        String operation = job.getConfiguration().get("pro.projection.operation",  "project");
        
        if (operation.startsWith("group") || operation.startsWith("order")) {
        	//group by
            job.setMapperClass(Projection.ProjectionMapper.class);
            job.setReducerClass(Projection.ProjectionReducer.class);

            job.setMapOutputKeyClass(Tuple.class);
            job.setMapOutputValueClass(Text.class);

            int numReducer = job.getConfiguration().getInt("pro.num.reducer", -1);
            numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
            job.setNumReduceTasks(numReducer);
        } else {
        	//simple projection
            job.setMapperClass(Projection.SimpleProjectionMapper.class);
        }
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SimpleProjectionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private int  keyField;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private AttributeFilter attrFilter;
        private RowColumnFilter rowColFilter = new RowColumnFilter();
        private boolean idIncluded;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	keyField = config.getInt("pro.key.field", 0);
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim", ",");
        	
        	//filter expression delimetrs
           	setFilterExprDelimeter(config);
        	
        	//projection
        	projectionFields = Utility.intArrayFromString(config.get("pro.projection.field"),fieldDelimRegex );
        	if (null == projectionFields) {
        		//projected field from the output of another MR
        		projectionFields = findIncludedColumns(config, rowColFilter);
        	}
        	idIncluded = config.getBoolean("pro.id.incuded.in.projection", true);
        	
        	//selection
        	String selectFilter = config.get("pro.select.filter");
        	if (null != selectFilter) {
           		attrFilter = buildSelectFilter(config, selectFilter,  rowColFilter);
        	} 
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            //only if filter matched
            if (null == attrFilter || attrFilter.evaluate(items)) {
            	if (idIncluded) {
            		outVal.set(Utility.extractFields(items , projectionFields, fieldDelimOut, false));
            	} else {
            		outVal.set(items[keyField] + fieldDelimOut +  Utility.extractFields(items , projectionFields, 
            				fieldDelimOut, false));
            	}
            	context.write(NullWritable.get(), outVal);
            }
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class ProjectionMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
		private int  keyField;
		private boolean includeKey;
		private int[]  projectionFields;
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private List<Pair<Integer, Boolean>> orderByFieldOrdinals;
        private boolean groupBy;
        private int[] groupByFieldOrdinals;
        private BaseAttributeFilter attrFilter;
        private RowColumnFilter rowColFilter = new RowColumnFilter();
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	fieldDelimOut = config.get("field.delim.out", ",");
        	
        	String operation = config.get("pro.projection.operation", "project");
        	groupBy = operation.startsWith("group");
        	if (groupBy) {
        		//group by
        		groupByFieldOrdinals = Utility.assertIntArrayConfigParam(config, "pro.group.by.field.ordinals", 
        				Utility.configDelim, "missing group by field ordinals");
        	} else {
            	//order by
            	int orderByField = config.getInt("pro.orderBy.field", -1);
            	boolean isOrderByFieldNumeric = config.getBoolean("pro.orderBy.filed.numeric", false);
            	if (-1 == orderByField) {
            		//multiple order by fields
    	        	orderByFieldOrdinals = Utility.getIntBooleanList(config, "pro.orderBy.fields", Utility.configDelim, Utility.configSubFieldDelim);
    	        	if (null != orderByFieldOrdinals && orderByFieldOrdinals.isEmpty()) {
    	        		throw new IllegalStateException("failed to process order by field configuration");
    	        	}
            	} else {
            		//one order by field
            		orderByFieldOrdinals = new ArrayList<Pair<Integer, Boolean>>();
            		orderByFieldOrdinals.add(new Pair<Integer, Boolean>(orderByField, isOrderByFieldNumeric));
            	}
        	}
        	
        	keyField = config.getInt("pro.key.field", 0);
        	includeKey = config.getBoolean("pro.include.key", true);
        	
        	//filter expression delimetrs
        	setFilterExprDelimeter(config);
        	
        	projectionFields = Utility.intArrayFromString(config.get("pro.projection.field"),fieldDelimRegex );
        	if (null == projectionFields) {
        		//projected field from the output of another MR
        		projectionFields = findIncludedColumns(config, rowColFilter);
        	}
        	
        	//selection
        	String selectFilter = config.get("pro.select.filter");
        	if (null != selectFilter) {
        		attrFilter = buildSelectFilter(config, selectFilter,  rowColFilter);
         	}
       }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex, -1);
            
            //only if filter matched
            if (null == attrFilter || attrFilter.evaluate(items)) {
	        	outKey.initialize();
        		if (includeKey) {
        			outKey.add(items[keyField]);
        		}
	            if (null != orderByFieldOrdinals) {
	            	//order by
	            	for (Pair<Integer, Boolean> field : orderByFieldOrdinals) {
	            		if (field.getRight()) {
		               		outKey.add(Double.parseDouble(items[field.getLeft()]));
	            		} else {
		               		outKey.add(items[field.getLeft()]);
	            		}
	            	}
	            } else {
	            	//group by
	            	outKey.addArrayElements(items, groupByFieldOrdinals);
	            }
	        	outVal.set(BasicUtils.extractFields(items , projectionFields, fieldDelimOut, false));
	        	context.write(outKey, outVal);
            }
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class ProjectionReducer extends Reducer<Tuple, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private RedisCache redisCache;
		private String aggregateValueKeyPrefix;
		private String[] aggrFunctions;
		private Map<String, Object> aggregateValues = new HashMap<String, Object>();
		private Map<String, Object> aggregateValuesMax = new HashMap<String, Object>();
		private List<String> strValues = new ArrayList<String>();
		private List<Double> doubleValues = new ArrayList<Double>();
		private Set<String> strValuesSet = new HashSet<String>();
		private double sum;
		private double sqSum;
		private List<String> sortedValues = new ArrayList<String>();
		private int limitTo;
		private boolean formatCompact;
		private double  stdDev;
		private boolean useRank;
		private boolean useRedisCache;
		private boolean groupBy;
		private double max;
		private double min;
		private boolean sumDone;
		private int outputPrecision;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	String operation = config.get("pro.projection.operation", "project");
        	groupBy = operation.startsWith("group");
       	
        	if (!StringUtils.isBlank(config.get("pro.aggregate.functions"))) {
        		if (groupBy) {
	        		aggrFunctions = Utility.stringArrayFromString(config, "pro.aggregate.functions", Utility.configDelim);
	        		useRedisCache = config.getBoolean("pro.use.redis.cache", true);
	        		if (useRedisCache) {
	        			aggregateValueKeyPrefix = config.get("pro.aggregate.value.key.prefix");
	        			redisCache = RedisCache.createRedisCache(config, "ch");
	        		}
        		} else {
        			throw new IllegalStateException("invalid to aggregate without group by");
        		}
        	} else {
        		//don't allow group by without aggregation
        		if (groupBy) {
        			throw new IllegalStateException("invalid to have group by without aggregation");
        		}
        	}
        	//sortOrderAscending = config.getBoolean("pro.sort.order.ascending", true);
        	limitTo = config.getInt("pro.limit.to", -1);
        	formatCompact = config.getBoolean("pro.format.compact", true);
        	useRank = config.getBoolean("pro.use.rank", false);
        	outputPrecision = config.getInt("pro.output.precision", 3);
       }

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (useRedisCache && null != aggrFunctions) {
				for (String fun : aggrFunctions) {
					redisCache.put(aggregateValueKeyPrefix + "." + fun ,  "" + aggregateValuesMax.get(fun), true);
				}
			}
		}
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		if (null != aggrFunctions) {
    			//aggregate functions
        		stBld.delete(0, stBld.length());
        		stBld.append(key.withDelim(fieldDelim).toString());

        		strValues.clear();
    			doubleValues.clear();
    			strValuesSet.clear();
    			aggregateValues.clear();
    			sum = 0;
    			sumDone = false;
    			sqSum = 0;
    			
    			//store values
	        	for (Text value : values){
	        		strValues.add(value.toString());
	        	}    			
	        	
	        	//all aggregate functions but limited to one variable
	        	for (int i = 0; i <  aggrFunctions.length; ++i) {
	        		if (aggrFunctions[i].equals("count")) {
	        			//count
	        			aggregateValues.put("count", strValues.size());
	        		} else if (aggrFunctions[i].equals("uniqueCount")) {
	        			//unique count
	        			for (String stVal : strValues) {
	        				strValuesSet.add(stVal);
	        			}
	        			aggregateValues.put("uniqueCount", strValuesSet.size());
	        		} else if (aggrFunctions[i].equals("sum")) {
	        			//sum
	        			if (!sumDone) {
	        				doSum();
	        			}
	        			aggregateValues.put("sum", sum);
	        		} else if (aggrFunctions[i].equals("average")) {
	        			//average
	        			if (!sumDone) {
		        			doSum();
	        			}
	        			aggregateValues.put("average", sum / doubleValues.size());
	        		} else if (aggrFunctions[i].equals("max")) {
	        			//max
		        		doMax();
	        			aggregateValues.put("max", max);
	        		} else if (aggrFunctions[i].equals("min")) {
	        			//min
		        		doMin();
	        			aggregateValues.put("min", min);
	        		} else if (aggrFunctions[i].equals("stdDev")) {
	        			//standard deviation
	        			Double average = null;
	        			if (aggregateValues.containsKey("average")) {
	        				average = (Double)aggregateValues.get("average");
	        			} else {
	        				if (!sumDone) {
	        					doSum();
	        				}
	        				average = sum / doubleValues.size();
	        			}
		        		doSqSum();
	        			stdDev = (double)sqSum / doubleValues.size() -  average * average;
	        			stdDev = Math.sqrt(stdDev);
	        			aggregateValues.put("stdDev", stdDev);
	        		}
	        	}
	        	
	        	//all aggregate values
	        	for (String fun : aggrFunctions) {
	        		Object aggrVal = aggregateValues.get(fun);
	        		Object aggrValMax = aggregateValuesMax.get(fun);
	        		if (aggrVal instanceof Integer) {
	        			Integer aggrValInt = (Integer)aggrVal;
	        			stBld.append(fieldDelim).append(aggrValInt);
	        			
	        			if (useRedisCache) {
		        			if (null == aggrValMax) {
		        				aggregateValuesMax.put(fun, aggrValInt);
		        			} else {
		        				Integer aggrValMaxInt = (Integer)aggrValMax;
		        				if (aggrValInt > aggrValMaxInt) {
		        					aggregateValuesMax.put(fun, aggrValInt);
		        				}
		        			}
	        			}
	        		} else {
	        			Double aggrValDouble = (Double)aggrVal;
	        			stBld.append(fieldDelim).append(BasicUtils.formatDouble(aggrValDouble, outputPrecision));
	        			
	        			if (useRedisCache) {
		        			if (null == aggrValMax) {
		        				aggregateValuesMax.put(fun, aggrValDouble);
		        			} else {
		        				Double aggrValMaxDouble = (Double)aggrValMax;
		        				if (aggrValDouble > aggrValMaxDouble) {
		        					aggregateValuesMax.put(fun, aggrValDouble);
		        				}
		        			}
	        			}
	        		}
	        	}
	        	
	        	outVal.set(stBld.toString());
				context.write(NullWritable.get(), outVal);
    		}  else {
       			//actual values
    			 if (formatCompact) {
    				emitCompactFormat(key,  values, context);
    			} else {
    				emitLongFormat(key,  values, context);
    			}
    		}
    	}

    	/**
    	 * emits actual values in compact format
    	 * @param key
    	 * @param values
    	 * @param context
    	 * @throws InterruptedException 
    	 * @throws IOException 
    	 */
    	private void emitCompactFormat(Tuple key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
			//actual values
    		stBld.delete(0, stBld.length());
    		stBld.append(key.withDelim(fieldDelim).toString());
			int i = 0;
        	for (Text value : values){
        		if (i == limitTo) {
        			break;
        		}
    	   		stBld.append(fieldDelim).append(value);
    	   		++i;
        	}    		
    		
	       	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
    	}
    	
    	/**
    	 * emits actual values in long format
    	 * @param key
    	 * @param values
    	 * @param context
    	 * @throws InterruptedException 
    	 * @throws IOException 
    	 */
    	private void emitLongFormat(Tuple key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
			//actual values
			int i = 0;
        	for (Text value : values){
        		if (i == limitTo) {
        			break;
        		}
        		stBld.delete(0, stBld.length());
        		stBld.append(key.withDelim(fieldDelim).toString());
        		if (useRank) {
        			//rank
        			stBld.append(fieldDelim).append(i+1);
        		} else {
        			//actual value
        			stBld.append(fieldDelim).append(value);
        		}
    	       	outVal.set(stBld.toString());
    			context.write(NullWritable.get(), outVal);
    	   		++i;
        	}    		
    	}

    	/**
    	 * 
    	 */
    	private void doSum() {
			if (doubleValues.isEmpty()) {
				initializeDoubleValues();
			}
			for (double doubleVal : doubleValues) {
				sum += doubleVal;
			}
			sumDone = true;
    	}

    	/**
    	 * 
    	 */
    	private void doSqSum() {
			if (doubleValues.isEmpty()) {
				initializeDoubleValues();
			}
			for (double doubleVal : doubleValues) {
				sqSum += doubleVal * doubleVal;
			}
    	}
    	
    	/**
    	 * 
    	 */
    	private void doMax() {
			if (doubleValues.isEmpty()) {
				initializeDoubleValues();
			}
			max = Double.MIN_VALUE;
			for (double doubleVal : doubleValues) {
				if (doubleVal > max) {
					max = doubleVal;
				}
			}    		
    	}

    	/**
    	 * 
    	 */
    	private void doMin() {
			if (doubleValues.isEmpty()) {
				initializeDoubleValues();
			}
			min = Double.MAX_VALUE;
			for (double doubleVal : doubleValues) {
				if (doubleVal < min) {
					min = doubleVal;
				}
			}    		
    	}

     	
    	/**
    	 * 
    	 */
    	private void initializeDoubleValues() {
			for (String stVal : strValues) {
				doubleValues.add(Double.parseDouble(stVal));
			}
    	}
    }
    
    /**
     * @param config
     */
    private static void setFilterExprDelimeter(Configuration config) {
    	String filterConjunctDelim = config.get("pro.filter.conjunct.delim");
    	if (null != filterConjunctDelim) {
    		AttributeFilter.setConjunctSeparator(filterConjunctDelim);
    	}
    	String filterDisjunctDelim = config.get("pro.filter.disjunct.delim");
    	if (null != filterDisjunctDelim) {
    		AttributeFilter.setDisjunctSeparator(filterDisjunctDelim);
    	}
    }
    
    /**
     * @param config
     * @param selectFilter
     * @param rowColFilter
     * @return
     * @throws IOException
     */
    private static AttributeFilter buildSelectFilter(Configuration config, String selectFilter, RowColumnFilter rowColFilter) 
    	throws IOException {
    	AttributeFilter attrFilter = null;
		String notInSetName = config.get("pro.operator.notin.set.name");
		String inSetName = config.get("pro.operator.in.set.name");
		if (null == notInSetName && null == inSetName) {
			//dnf expression
			Map<String, Object> udfContext = getUdfConfiguration (config);
			attrFilter = udfContext == null ? new AttributeFilter(selectFilter) : 
				new AttributeFilter(selectFilter, udfContext);
		} else {
			if (null != notInSetName) {
           		//bulk data from external source for notin operator
    			attrFilter =  new AttributeFilter();
        		createExcludedRowsContext(config,  rowColFilter, attrFilter, selectFilter);
			} else {
           		//bulk data from external source for in operator
				
			}
    	}
    	return attrFilter;
    }
 
    /**
     * @param config
     * @param rowColFilter
     * @return
     * @throws IOException
     */
    private static int[] findIncludedColumns(Configuration config, RowColumnFilter rowColFilter) throws IOException {
		//projected field from the output of another MR
    	String fileterFieldDelimRegex = config.get("pro.filter.field.delim.regex", ",");
		InputStream colStream = Utility.getFileStream(config, "pro.exclude.columns.file");
		if (null == colStream) {
			throw new IllegalStateException("error aceesing excluded column file");
		}
		rowColFilter.processColumns(colStream, fileterFieldDelimRegex);
		int numCols = Utility.assertIntConfigParam(config, "pro.num.fields", "missing configuration for number of fields");
		return rowColFilter.getIncludedColOrdinals(numCols);
    }
    
    /**
     * @param config
     * @param rowColFilter
     * @param attrFilter
     * @throws IOException
     */
    private static void createExcludedRowsContext(Configuration config, RowColumnFilter rowColFilter, 
    		BaseAttributeFilter attrFilter, String selectFilter) throws IOException {
    	String fileterFieldDelimRegex = config.get("pro.filter.field.delim.regex", ",");
		String notInSetName = config.get("pro.operator.notin.set.name");
		
		//notin operator with out of band set values
		InputStream rowStream = Utility.getFileStream(config, "pro.exclude.rows.file");
		if (null == rowStream) {
			throw new IllegalStateException("error aceesing excluded row file");
		}
		rowColFilter.processRows(rowStream, fileterFieldDelimRegex);
		String[] exclRowKeys = rowColFilter.getExcludedRowKeys();
		Map<String, Object> setOpContext = new HashMap<String, Object>();
		setOpContext.put(notInSetName, BasicUtils.generateSetFromArray(exclRowKeys));
		attrFilter.withContext(setOpContext).build(selectFilter);;
    }    
    
    /**
     * @param config
     * @return
     * @throws IOException
     */
    private static Map<String, Object> getUdfConfiguration (Configuration config) throws IOException {
		Map<String, Object> udfContext = null;
    	String[] keys = config.getStrings("pro.udf.config.params");
    	if (null != keys) {
    		udfContext = new HashMap<String, Object>();
	    	for (String key : keys) {
	    		String value = config.get(key);
	    		//System.out.println("udf  key:" + key + " value:" + value);
	    		udfContext.put(key, value);
	    	}
    	}
    	return udfContext;
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Projection(), args);
        System.exit(exitCode);
	}

}
