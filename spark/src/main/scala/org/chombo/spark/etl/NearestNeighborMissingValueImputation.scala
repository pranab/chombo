/*
 * chombo-spark: etl on spark
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

package org.chombo.spark.etl

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.distance.InterRecordDistance
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.chombo.stats.NonParametricDistrRejectionSampler

/**
 * Imputes missing field values based on nearest neighbors
 * @author pranab
 *
 */
object NearestNeighborMissingValueImputation extends JobConfiguration {
   /**
    * @param args
    * @return
    */
   def main(args: Array[String])  {
	   val appName = "nearestNeighborMissingValueImputation"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val neighborCount = getMandatoryIntParam(appConfig, "neighbor.count", "missing neighbor count")
	   val recLen = getMandatoryIntParam(appConfig, "rec.len", "missing record length parameter")
	   val distrFactor = getMandatoryDoubleParam(appConfig, "distr.factor")
	   val classValueOrd = getOptionalIntParam(appConfig, "class.value.ord")
	   val minNeighborCount = classValueOrd match {
	     case Some(ord : Int) => 
	       getConditionalMandatoryIntParam(true, appConfig, "min.neighbor.count", "miising min neighbor count")
	     case None => 0
	   }
	   val sampleNeighbor = getBooleanParamOrElse(appConfig, "", true)
	   val genAttrSchemaPath = getMandatoryStringParam(appConfig, "gen.attr.schema.path")
	   val genAttrSchema = BasicUtils.getGenericAttributeSchema(genAttrSchemaPath)
	   val precision = this.getIntParamOrElse(config, "float.precision", 3)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   val sampler = new NonParametricDistrRejectionSampler[Integer]()
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val fixedRecs = data.map(line => {
		   sampler.initialize()
		   val items = line.split(fieldDelimIn, -1)
		   val srcRec = items.slice(0, recLen)
		   val neighbors = ArrayBuffer[Array[String]]()
		   var offset = recLen
		   
		   //all neighbors
		   for (i <- 0 to neighborCount-1) {
		     val neRec = items.slice(offset, offset+recLen)
		     val toInclude = classValueOrd match {
		     	case Some(ord : Int) => {
		     	  val srcClassVal = srcRec(ord)
		     	  val neClassVal = neRec(ord)
		     	  srcClassVal.equals(neClassVal)
		     	}
		     	case None => true
		     }	
		     if (toInclude) {
		    	 val dist = items(offset + recLen).toDouble
		    	 val distr = Math.pow(1 / dist, distrFactor)
		    	 neighbors += neRec
		    	 sampler.add(i, distr)
		     }
		     offset = offset + recLen + 1
		   }
		   
		   //whether to fill missing value if class label matched check min neighborhood size
		   val fillMissingValue = classValueOrd match {
		     	case Some(ord : Int) => neighbors.size >= minNeighborCount
		     	case None => true
		    }	
		   
		   //fill missing values
		   if (fillMissingValue) {
			   srcRec.zipWithIndex.foreach(r => {
			     val field = r._1
			     val idx = r._2
			     if (field.isEmpty()) {
			       if (sampleNeighbor) {
			         //sampled 
			         val sampledNe = sampler.sample()
			         val neRec = neighbors(sampledNe)
			         srcRec(idx) = neRec(idx)
			       } else {
			         //expected value
			         if (genAttrSchema.areCategoricalAttributes(idx)) {
			           //mode
			           val modeNe = sampler.getMode();
			           val neRec = neighbors(modeNe)
			           srcRec(idx) = neRec(idx)
			         } else if (genAttrSchema.areNumericalAttributes(idx)){
			           //expected value
			           var exVal = 0.0
			           for (key <-  sampler.getNormDistr().keySet().asScala) {
			             val distr = sampler.getNormDistr().get(key)
			             val fval = neighbors(key)(idx).toDouble
			             exVal = exVal + fval * distr
			           }
			           
			           val fVal = if (genAttrSchema.getAttributes().get(idx).isDouble()) {
			             BasicUtils.formatDouble(exVal, precision)
			           } else {
			             Math.round(exVal).toString
			           }
			           srcRec(idx) = fVal
			         }
			       }
			     }
			   })
		   }
		   
		   srcRec.mkString(fieldDelimOut)
	   })
	   
	   if (debugOn) {
	     val fixedRecsCol = fixedRecs.collect
	     fixedRecsCol.slice(0,20).foreach(d => {
	       println(d)
	     })
	   }	
	   
	   if (saveOutput) {
	     fixedRecs.saveAsTextFile(outputPath)
	   }
	   
   }

}