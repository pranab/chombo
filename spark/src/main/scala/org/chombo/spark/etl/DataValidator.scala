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
import org.chombo.validator.ValidatorFactory
import com.typesafe.config.Config
import org.chombo.validator.Validator
import org.chombo.util.ProcessorAttributeSchema
import org.chombo.stats.NumericalAttrStatsManager
import org.chombo.stats.MedianStatsManager
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.chombo.util.BasicUtils

/**
 * @author pranab
 *
 */
abstract trait ValidatorRegistration {
  def registerValidators(fieldOrd : Int, validators : Validator*)
}

/**
 * @author pranab
 *
 */
abstract trait ValidatorBuilder {
	def createValidators(config : Config, validator : ValidatorRegistration) 
}

/**
@author pranab
 *
* */
object DataValidator extends JobConfiguration  with ValidatorRegistration {
  val validationContext = new java.util.HashMap[String, Object]()
  val mutValidators : scala.collection.mutable.HashMap[Int, Array[Validator]]   = scala.collection.mutable.HashMap()
  lazy val validators :  Map[Int, Array[Validator]]   = mutValidators.toMap
    
    
 /**
 * @param args
 * @return
 */
   def main(args: Array[String]) {
       val appName = "dataValidator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val valTagSeparator = getStringParamOrElse(appConfig, "val.tag.separator", ";")
	   val filterInvalidRecords = getBooleanParamOrElse(appConfig, "filter.invalid.records", true)
	   val outputInvalidRecords = getBooleanParamOrElse(appConfig, "output.invalid.records", true)
	   val invalidRecordsOutputFile = if (outputInvalidRecords) {
	     getMandatoryStringParam(appConfig, "invalid.records.output.file", "missing invalid output file path")
	   } else {
	     ""
	   }
	   val tagWithFailedValidator = getBooleanParamOrElse(appConfig, "tag.with.failed.validator", true)
	   val invalidFieldMask = getStringParamOrElse(config, "invalid.field.mask", "??????")
	   
	   val validationSchema = BasicUtils.getProcessingSchema(
	       getMandatoryStringParam(appConfig, "schema.file.path", "missing schema file path configuration"))
	   val validatorConfig = appConfig
	   val customValidFactoryClass = 
	     if (appConfig.hasPath("custom.valid.factory.class"))
	       appConfig.getString("custom.valid.factory.class")
	     else
	       null
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	     
	   ValidatorFactory.initialize(customValidFactoryClass, validatorConfig)
	   val ordinals =  validationSchema.getAttributeOrdinals()
	   
	   //initialize stats manager
	   val statsFilePath = getOptionalStringParam(appConfig, "stats.file.path")
	   val statsManager = statsFilePath match {
	     case Some(path:String) => {
	       val statsManager = new NumericalAttrStatsManager(path, ",", true)
	       validationContext.put("stats",  statsManager)
	       Some(statsManager)
	     }
	     case None => None
	   }
       
       //initialize median stats manager
	   val medStatsFilePath = this.getOptionalStringParam(appConfig, "med.stats.file.path")
	   val medStatsManager = medStatsFilePath match {
	     case Some(path:String) => {
	       val modStatsFilePath = getMandatoryStringParam(appConfig, "mad.stats.file.path", 
	           "missing stats file path configuration")
	       val idOrdList = getMandatoryIntListParam(appConfig, "id.ordinals", "missing id ordinals configuration")
	       val idOrds = BasicUtils.fromListToIntArray(idOrdList)
	       val statsManager = new MedianStatsManager(path, modStatsFilePath, ",",  idOrds)
	       validationContext.put("stats",  statsManager)
	       Some(statsManager)
	     }
	     case None => None
	   }
	   
	  //create field validators
      validationSchema.getAttributes().asScala.foreach( attr  => {
    	  	if (null != attr.getValidators()) {
    	  		val validatorTags =  attr.getValidators().asScala.toArray
    	  		createValidators(appConfig, validatorTags, attr.getOrdinal(), validationSchema, 
    	  		    mutValidators, statsManager, medStatsManager)
    	  	}
      })
      
      //create row validators
      val rowValidatorTags = validationSchema.getRowValidators().asScala.toArray
      val rowValidators = createRowValidators(appConfig, rowValidatorTags, fieldDelimIn)
	   
	  if (debugOn) {
	     validators.foreach(kv => {
	       println("field: " + kv._1 + " num validators:" + kv._2.length )
	     })
	   }
	   
	   //broadcast validator
	   val brValidators = sparkCntxt.broadcast(validators)
	   
	   //broadcast row validators
	   val brRowValidators = sparkCntxt.broadcast(rowValidators)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath)

	   //apply validators to each field in each line to create RDD of tagged records
	   val taggedData = data.map(line => {
	     val items = line.split(fieldDelimIn, -1)
	     val itemsZipped = items.zipWithIndex
	    
	     //apply all validators for the field
	     val valMap = brValidators.value
	     val taggedItems = itemsZipped.map(z => {
	    	val valListOpt = valMap.get(z._2)
	    	
	    	val taggedField = valListOpt match  {
	    	  case Some(valList: Array[Validator]) => {
	    		  val valStatuses = valList.map(validator => {
	    			  val status = validator.isValid(z._1)
	    			  (validator.getTag(), status)
	    		  })
	    		  
	    		  if (debugOn) {
	    		    println("next field index value: " + z._2 + "  " + z._1)
	    		    valStatuses.foreach(vs => println("validator: " + vs._1 + " status:" + vs._2))
	    		  }
	    	
	    		  //only failed validators
	    		  val failedValidators = valStatuses.filter(s => {
	    		   !s._2
	    		  })
	    		  val field = 
	    	       	if (failedValidators.isEmpty)
	    	    	   z._1
	    	    	 else {
	    	    	   if (tagWithFailedValidator) {
	    	    		   val fvals = failedValidators.map(vs => vs._1)
	    	    		   if (debugOn) {
	    	    			   println("failed validators " + fvals.mkString(","))
	    		           }
	    	    		   
	    	    		   z._1 + valTagSeparator + fvals.mkString(valTagSeparator)
	    	    	   } else {
	    	    	     invalidFieldMask
	    	    	   }
	    	    	 }
	    	    field
	    	  }
	    	  
	    	  case None =>  z._1
	    	}
	    	taggedField
	    })
	 
	    val rec = taggedItems.mkString(fieldDelimOut)
	    
	    //apply row validators
	    val rowValidators = brRowValidators.value
	    val rowValStatuses = rowValidators.map(validator => {
	      val status = validator.isValid(line)
	      (validator.getTag(), status)
	    })
	    val failedRowValidators = rowValStatuses.filter(s => {
	    	!s._2
	    }).map(vs => vs._1)
	    	
	    //tag row validators at the end of the record
	    val taggedRec = 
	    	if (failedRowValidators.isEmpty)
	    		rec
	    	else 
	    		rec + valTagSeparator + valTagSeparator + failedRowValidators.mkString(valTagSeparator)
	    	
	    taggedRec
	  })
	  
	  //failed validator annotated record
	  taggedData.cache

	 //filter valid data
	 if (filterInvalidRecords) {
	     //only valid records
		 val validData = taggedData.filter(line => !line.contains(valTagSeparator) && !line.contains(invalidFieldMask))
		 validData.saveAsTextFile(outputPath)
	 } else {
	   //all records
	   taggedData.saveAsTextFile(outputPath)
	 }
	  
	 //output invalid data
	 if (outputInvalidRecords){
		val invalidData = taggedData.filter(line => line.contains(valTagSeparator) || line.contains(invalidFieldMask))
		invalidData.saveAsTextFile(invalidRecordsOutputFile)
	 }
   }
   
   /**
   * @param config
   * @param valTags
   * @param ord
   * @param validatorConfig
   * @param validationSchema
   */
   private  def createValidators(config : Config, valTags : Array[String], ord : Int,
       validationSchema :ProcessorAttributeSchema, 
       mutValidators : scala.collection.mutable.HashMap[Int, Array[Validator]],
       statsManager: Option[NumericalAttrStatsManager],
       medStatsManager: Option[MedianStatsManager])  {
	   val  prAttr = validationSchema.findAttributeByOrdinal(ord)
	   val validators = valTags.map(tag => {
		    val validator = tag match {
		     case "zscoreBasedRange" => {
		        validationContext.clear()
		        statsManager match {
		          case Some(stMan : NumericalAttrStatsManager) => validationContext.put("stats",  stMan)
		          case None => throw new IllegalStateException("missing attribute stats manager")
		        }
		    	ValidatorFactory.create(tag, prAttr, validationContext)
		     }
		     
		     case "robustZscoreBasedRange" => {
		        validationContext.clear()
		        medStatsManager match {
		          case Some(stMan : MedianStatsManager) => validationContext.put("stats",  stMan)
		          case None => throw new IllegalStateException("missing attribute median stats manager")
		        }
		    	 ValidatorFactory.create(tag, prAttr, validationContext)
		     }
		    
		     case tag:String => {
		       val validatorConfig = 
		         if (config.hasPath(tag))
		        	 config.getConfig(tag)
		         else 
		        	 null
		       ValidatorFactory.create(tag, prAttr,  validatorConfig)
		     }
		   }
		   validator 
	   })
	   
	   //add validators to map
	   mutValidators += ord -> validators
   }
   
  /**
  * @param config
  * @param valTags
  * @return
  */
  private def createRowValidators(config : Config, valTags : Array[String], fieldDelim : String) : Array[Validator] = {
     val validators = valTags.map(valTag => {
       config.hasPath(valTag) match {
         case true => {
           ValidatorFactory.create(valTag, config.getConfig(valTag), fieldDelim);
         }
         case false => {
           throw new IllegalStateException("missing configuration for row validator " + valTag)
         }
       }
     })
     validators
   }
  
    /**
     * @param fieldOrd
     * @param transformers
     */
    def registerValidators(fieldOrd : Int, validators : Validator*) {
      val valArr = validators.toArray
      mutValidators += fieldOrd -> valArr
    }
  
}
