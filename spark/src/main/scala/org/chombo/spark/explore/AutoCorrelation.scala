package org.chombo.spark.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.spark.common.Record
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer


/**
 * Auto correlation
 * @author pranab
 */
object AutoCorrelation extends JobConfiguration {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "autoCorrelation"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "", "missing sequence filed ordinal")
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	  val numAttrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals").asScala.toArray
	  val corrLags = getMandatoryIntListParam(appConfig, "coor.lags", "missing correlation lags").asScala.toArray
	  val meanValues = this.getMandatoryIntDoubleMapParam(appConfig, "mean.values", "missing mean values")
	  val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	  
	  //key length
	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None =>
	  }
	  keyLen += 4
	  
	  val data = sparkCntxt.textFile(inputPath)
	  val keyedData = data.flatMap(line => {
		   val items = line.split(fieldDelimIn, -1)
		   val seq = items(seqFieldOrd).toInt
		   val recs = ArrayBuffer[(Record, Record)]()
		   
		   //each quant field
		   numAttrOrdinals.foreach(fld => {
		     val fieldVal = items(fld).toDouble
		     
		     //each lag
		     corrLags.foreach(lag => {
		       val laggedSeq = seq - lag
		       val aheadSeq = seq + lag
		       val value = Record(3)
		       value.addInt(seq)
		       value.addInt(fld)
		       value.addDouble(fieldVal)
		       
		       //pair with lagged
		       val lKey = buildKey(fld, lag, laggedSeq, seq, keyLen, keyFieldOrdinals, items)
		       recs += ((lKey, value))
		       
		       //pair with ahead
		       val aKey = buildKey(fld, lag, seq, aheadSeq, keyLen, keyFieldOrdinals, items)
		       recs += ((aKey, value))
		     })
		   })
		   
		   recs
	  })

	  var corRecs = keyedData.groupByKey.mapValues(v => {
	    val a = v.toArray
	    if (a.length == 2) {
	      val value = Record(2)
	      val pair = if (a(0).getInt(0) < a(1).getInt(0)) {
	        (a(0), a(1))
	      } else {
	        (a(1), a(0))
	      }
	      val fldOrd = pair._1.getInt(1)
	      val mean = meanValues.get(fldOrd)
	      val lagDiff = pair._1.getDouble(2) - mean
	      val curDiff = pair._2.getDouble(2) - mean
	      value.addDouble(curDiff * lagDiff)
	      value.addDouble(curDiff * curDiff)
	      value
	    } else {
	      val rec = Record(1)
	      rec.addString("x")
	      rec
	    }
	  })
	  
	  //filter out invalid ones
	  corRecs = corRecs.filter(v => {v._2.size == 2})
	  
	  //remove sequences from key
	  corRecs = corRecs.map(kv => {
	    val key = kv._1
	    val newKey = Record(key, 0, key.size - 2)
	    (newKey, kv._2)
	  })
	  
	  //aggregate
	  corRecs = corRecs.reduceByKey((v1,v2) => {
	    val rec = Record(2)
	    rec.addDouble(v1.getDouble(0) + v2.getDouble(0))
	    rec.addDouble(v1.getDouble(1) + v2.getDouble(1))
	    rec
	  }) 
	  
	  val autoCor = corRecs.mapValues(v => v.getDouble(0) / v.getDouble(1))
	  
	  if (debugOn) {
	     autoCor.collect.foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     autoCor.saveAsTextFile(outputPath)
	  }
	  
   }
   
   /**
   * @param appName
   * @param config
   * @param includeAppConfig
   * @return
   */ 
   def buildKey(fld:Int, lag:Integer, firstSeq: Int, secondSeq: Int, keyLen:Int, keyFieldOrdinals:Option[Array[Integer]], 
      items: Array[String]) : Record = {
       val key = Record(keyLen)
       keyFieldOrdinals match {
         case Some(fields : Array[Integer]) => {
           for (kf <- fields) {
             key.addString(items(kf))
           }
         }
         case None =>
       }
       key.addInt(fld)
       key.addInt(lag)
       key.addInt(firstSeq)
       key.addInt(secondSeq)
       key
   }
   
}