/*
 `* chombo-spark: etl on spark
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


package org.chombo.spark.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.stats.HistogramStat
import org.chombo.stats.HistogramUtility
import java.io.FileInputStream
import scala.collection.mutable.Map
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.SeasonalAnalyzer
import org.chombo.stats.NumericalAttrStatsManager
import org.chombo.util.BasicUtils
import org.chombo.spark.common.GeneralUtility
import org.chombo.util.Pair

object NumericalAttrDistrStats extends JobConfiguration with SeasonalUtility with GeneralUtility{
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "numericalAttrDistrStats"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = toOptionalIntArray(keyFields)
	   var keyLen = getOptinalArrayLength(keyFieldOrdinals) + 1
	   val idLen = keyLen - 1
	   
	   //seasonal
	   val seasonalAnalysis = getBooleanParamOrElse(appConfig, "seasonal.analysis", false)

	   //stats file record key length
	   val statsKeyLen = idLen + (if (seasonalAnalysis) 2 else 0) + 1
	   
	   val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", "missing quant field ordinals").asScala.toArray

	   //bin widths
	   var keyBasedBinWidth = false;
	   val binWidthFile = getOptionalStringParam(appConfig, "bin.widthFile")
	   val binWidths = binWidthFile match {
	     case Some(filePath) => {
	       //based on key
	       keyBasedBinWidth = true
	       val binWidthFieldOrd = statsKeyLen
	       val binWidths = Map[Record, Double]()
	       val bwMap = BasicUtils.getKeyedValues(filePath, statsKeyLen, binWidthFieldOrd).asScala
	       println("bin width map size " + bwMap.size)
	       BasicUtils.getKeyedValues(filePath, statsKeyLen, binWidthFieldOrd).asScala.foreach(v => {
	         println("binwidths: " + v)
	         val binWidthKey = buildTypedKey(v._1, fieldDelimIn, seasonalAnalysis)
	         binWidths += {binWidthKey -> v._2}
	       })
	       binWidths.toMap
	     }
	     
	     case None  => {
		   //based on field ordinal
	       val binWidths = Map[Record, Double]()
		   numAttrOrdinals.foreach(ord => {
		     //attribute bin width tuple
		     val keyStr = "attrBinWidth." + ord
		     val binWidthKey = Record(1)
		     binWidthKey.addInt(ord.toInt)
		     val width = getMandatoryIntParam(appConfig, keyStr, "missing bin width")
		     binWidths += {binWidthKey -> width}
		   })
		   binWidths.toMap
	     }
	   }
	   
	   val extendedOutput = getBooleanParamOrElse(appConfig, "extended.output", true)
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	   
	   //distribution fitness algo
	   val distrFitnessAlgo = getStringParamOrElse(appConfig, "distr.fitnessAlgo", "none")
	   distrFitnessAlgo match {
	     case ("none") => 
	     case (algo:String) => {
	    	 val algos = Array[String]("chiSquare", "klDiverge")
	         assertStringMember(algo, algos, "invalid distribution fitness algorithm")
	     }
	   }
	  
	   var confIntervalFactor = -1.0
	   var chiSqureFailOnRangeCheck = false
	   if (distrFitnessAlgo.equals("chiSquare")) {
	     confIntervalFactor = getMandatoryDoubleParam(appConfig, "conf.intervalFactor", 
	         "missinginterval factor confidence ")
	     chiSqureFailOnRangeCheck = getBooleanParamOrElse(appConfig, "chiSqure.failOnRangeCheck", false)
	   }
	   
	   
	   //either sample distribution or sample mean and std deviation
	   val refDistrFilePath = getOptionalStringParam(appConfig, "reference.distrFilePath")
	   var statsValueMap = scala.collection.mutable.Map[String,Pair[java.lang.Double,java.lang.Double]]()
	   refDistrFilePath match {
	     case Some (filePath) => 
	     case None => {
	       if (!distrFitnessAlgo.equals("none")) {
	         //mean and std deviation from stats output file
	    	 val refStatsFilePath = getMandatoryStringParam(appConfig, "ref.statsFilePath", "missing stats file path")
	    	 val meanFldOrd = statsKeyLen + 4
	    	 val stdDevFldOrd = statsKeyLen + 6
	    	 statsValueMap = BasicUtils.getKeyedValuePairs(refStatsFilePath, statsKeyLen, meanFldOrd, stdDevFldOrd).asScala
	       }
	     }
	   }
	   var statsValueImmMap = statsValueMap.toMap
	   val statsMap = updateMapKeys(statsValueImmMap, ((k:String) => {
	     buildTypedKey(k, fieldDelimIn, seasonalAnalysis)
	   }))
	   
	   
	   
	   //seasonal data
	   val partBySeasonCycle = getBooleanParamOrElse(appConfig, "part.bySeasonCycle", true)
	   val seasonalAnalyzers = if (seasonalAnalysis) {
		   val seasonalCycleTypes = getMandatoryStringListParam(appConfig, "seasonal.cycleType", 
	        "missing seasonal cycle type").asScala.toArray
	        val timeZoneShiftHours = getIntParamOrElse(appConfig, "time.zoneShiftHours", 0)
	        val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	        "missing time stamp field ordinal")
	        val timeStampInMili = getBooleanParamOrElse(appConfig, "time.inMili", true)
	        
	        val analyzers = seasonalCycleTypes.map(sType => {
	    	val seasonalAnalyzer = createSeasonalAnalyzer(this, appConfig, sType, timeZoneShiftHours, timeStampInMili)
	        seasonalAnalyzer
	    })
	    Some((analyzers, timeStampFieldOrdinal))
	   } else {
		   None
	   }
	   keyLen += (if (seasonalAnalysis) 2 else 0)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //key with record key and attr ordinal and value map of counts
	   var keyedRecs = data.flatMap(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val attrValCount = numAttrOrdinals.map(ord => {
		     //key
			 val attrKeyRec = keyFieldOrdinals match {
			     //with partition key and field ordinal
			     case Some(fields:Array[Int]) => {
			       val rec = Record(keyLen) 
			       populateFields(items, fields, rec) 
			       
			       //seasonality cycle
		           seasonalAnalyzers match {
		             case Some(seAnalyzers : (Array[SeasonalAnalyzer], Int)) => {
		            	 val timeStamp = items(seAnalyzers._2).toLong
		            	 val cIndex = SeasonalAnalyzer.getCycleIndex(seAnalyzers._1, timeStamp)
		            	 rec.addString(cIndex.getLeft())
		            	 rec.addInt(cIndex.getRight())
		             }
		             case None => 
			       }	  
			       
			       //quant field ordinal
			       rec.addInt(ord.toInt)
			       rec
			     }
			     //field ordinal only
			     case None => Record(1, ord.toInt)
			 }
		     
			 //value is histogram
			 val binWidthKey = if (keyBasedBinWidth) attrKeyRec else Record(1, ord.toInt)
			 //println("binWidths size " + binWidths.size)
			 //binWidths.foreach(v => println(v))
			 val binWidth = getMapValue(binWidths, binWidthKey, "missing bin width for key " + binWidthKey.toString)
		     //val binWidth = binWidths.get(binWidthKey).get
			 val attrValRec = new HistogramStat(binWidth)
		     attrValRec.
		     	withExtendedOutput(extendedOutput).
		     	withOutputPrecision(outputPrecision)
		     val attrVal = items(ord.toInt).toDouble
		     if (debugOn) {
		       //println("attrVal: " + attrVal)
		     }
		     attrValRec.add(attrVal)
		     (attrKeyRec, attrValRec)
		   })
		   
		   attrValCount
	   })
	   
	  //filter invalid seasonal index
	  keyedRecs  = if (seasonalAnalysis) {
	    keyedRecs.filter(v => {
	      val key = v._1
	      val ci = idLen + 1
	      key.getInt(ci) >= 0
	    })
	  } else {
	    keyedRecs
	  }
	   
	   //merge histograms and collect output
	   val stats = keyedRecs.reduceByKey((h1, h2) => h1.merge(h2))
	   val colStats = stats.collect
	    
	   if(debugOn)
	     println("done with distributions")
	     
	   //fitness
	   var withFitness = false
	   val modStats = refDistrFilePath match {
         case Some(filePath) => {
           //distr file path
	       val refStats = HistogramUtility.createHiostograms(new FileInputStream(filePath), keyLen, true)
	       val stats = refStats.asScala.map(kv => {
	         //last element of key is field ordinal
	         val rec = Record(kv._1)
	         rec.addInt(keyLen-1, kv._1(keyLen-1).toInt)
	         (rec, kv._2)
	       })
	       
	       //algos
	       val modStats = distrFitnessAlgo match {
	         case "chiSquare" =>  {
	           withFitness = true
			   colStats.map(v => {
		         val key = v._1
		         val refDistr = stats.get(key).get
		         val thisDistr = v._2
		         val fitnessScore = HistogramUtility.distrFittnessReferenceWithChiSquare(refDistr, thisDistr, confIntervalFactor, 
		             chiSqureFailOnRangeCheck)
		         val fitness = Record(3)
				 val fitted = fitnessScore.getLeft() < fitnessScore.getRight();
				 fitness.add(fitnessScore.getLeft(), fitnessScore.getRight(), fitted)
		         (v._1, v._2, fitness)
			   })	       
	         }
	         case "klDiverge" =>  {
	           withFitness = true
			   colStats.map(v => {
		         val key = v._1
		         val refDistr = stats.get(key).get
		         val thisDistr = v._2
		         val diverge = HistogramUtility.findKullbackLeiblerDivergence(refDistr, thisDistr)
		         val fitness = Record(2)
		         fitness.add(diverge.getLeft().toDouble, diverge.getRight().toInt)
		         (v._1, v._2, fitness)
			   })
	         }
	         case "none" => {
	           outputWithoutFitness(colStats)
	         }
	       }
           modStats
         } 
         
         case None => {  
             if (!statsValueMap.isEmpty) {
            	 //ref stats specified
	        	 val modStats = distrFitnessAlgo match {
	        	   case "chiSquare" =>  {
	        		 withFitness = true
	        		 colStats.map(v => {
				       val key = v._1
				       val thisDistr = v._2
				         
				       val stat = statsMap.get(key).get
				       val fitnessScore = HistogramUtility.distrFittnessNormalWithChiSquare(thisDistr, stat.getLeft(), stat.getRight(), 
				             confIntervalFactor, chiSqureFailOnRangeCheck)
				       val fitness = Record(3)
				       val fitted = fitnessScore.getLeft() < fitnessScore.getRight();
				       fitness.add(fitnessScore.getLeft(), fitnessScore.getRight(), fitted)
				       (v._1, v._2, fitness)
	        	     })
	        	   }
	        	   case "klDiverge" =>  {
	        	     //not supported yet
	        	     outputWithoutFitness(colStats)
	        	   }
	        	   case "none" => {
	        	     outputWithoutFitness(colStats)
	        	   }
	        	 }
	        	 modStats
             } else {
               outputWithoutFitness(colStats)
             }
   
         }
	   }
	   
	   if (debugOn) {
	     modStats.foreach(s => {
	       println("id:" + s._1)
	       println("distr:" + s._2.withSerializeBins(true).withOutputPrecision(outputPrecision))
	       println("fitness:" + s._3 )
	     })
	   }
	   
	   if (saveOutput) {
	     val stats = sparkCntxt.parallelize(modStats).map(v => {
	       val baseOutput = v._1.toString() + fieldDelimOut + v._2.withSerializeBins(true).withOutputPrecision(outputPrecision).toString() 
	       val fitness = v._3
	       if(!withFitness) {
	         baseOutput
	       } else {
	         baseOutput + fieldDelimOut + fitness.withFloatPrecision(outputPrecision).toString()
	       }
	     })
	     stats.saveAsTextFile(outputPath)
	   }
	   
   }
   
   
   /*
    * @param colStats
    */
   def outputWithoutFitness(colStats:Array[(Record, HistogramStat)]) : Array[(Record, HistogramStat, Record)] = {
       colStats.map(v => {
         val fitness = Record(1)
         fitness.add("none")
         (v._1, v._2, fitness)
       })
   }
   
   /**
  * @param key
  * @param fieldDelimIn
  * @param seasonality
  * @return
  */
  def buildTypedKey(key:String, fieldDelimIn:String, seasonality:Boolean) : Record = {
     val fields = BasicUtils.getTrimmedFields(key, fieldDelimIn)
     val len = fields.length
     val idLen = len - 1 - (if(seasonality) 2 else 0)
     val typedKey = Record(len)
     
     //id
     var k = 0
     for (i <- 0 to idLen - 1) {
       typedKey.addString(fields(k))
       k += 1
     }
     
     //seasonality
     if (seasonality) {
       typedKey.addString(fields(k))
       k += 1
       typedKey.addInt(fields(k).toInt)
       k += 1
     }
     
     //atr ordinal
     typedKey.addInt(fields(k).toInt)
     typedKey
   }
   
}