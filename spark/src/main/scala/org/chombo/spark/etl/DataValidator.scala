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
import org.chombo.util.NumericalAttrStatsManager
import org.chombo.util.MedianStatsManager
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
  var statsManager: Option[NumericalAttrStatsManager] = None
  var medStatManager : Option[MedianStatsManager] = None
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
	   
	   val invalidRecordsOutputFile = getOptionalStringParam(appConfig, "invalid.records.output.file")
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val valTagSeparator = getStringParamOrElse(appConfig, "val.tag.separator", ";")
	   val filterInvalidRecords = getBooleanParamOrElse(appConfig, "filter.invalid.records", true)
	   val outputInvalidRecords = getBooleanParamOrElse(appConfig, "output.invalid.records", true)
	   
	   val validationSchema = BasicUtils.getProcessingSchema(
	       getMandatoryStringParam(appConfig, "schema.file.path", "missing schema file path configuration"))
	   val validatorConfig = appConfig
	   val configClass = if (appConfig.hasPath("custom.valid.factory.class"))
	     appConfig.getString("custom.valid.factory.class")
	   else
	     null
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	     
	   ValidatorFactory.initialize(configClass, validatorConfig)
	   val ordinals =  validationSchema.getAttributeOrdinals()
	   
	   //initialize stats manager
	   val statsFilePath = getOptionalStringParam(appConfig, "stats.file.path")
	   statsFilePath match {
	     case Some(path:String) => getAttributeStats(path)
	     case None =>
	   }
	   val medStatsFilePath = this.getOptionalStringParam(appConfig, "med.stats.file.path")
	   medStatsFilePath match {
	     case Some(path:String) => {
	       val modStatsFilePath = getMandatoryStringParam(appConfig, "mad.stats.file.path", 
	           "missing stats file path configuration")
	       val idOrdList = getMandatoryIntListParam(appConfig, "id.ordinals", "missing id ordinals configuration")
	       val idOrds = BasicUtils.fromListToIntArray(idOrdList)
	       getAttributeMeds(path, modStatsFilePath, idOrds)
	     }
	     
	     case None =>
	   }
	   
	   //simple validators  
	   var foundPropConfigBasedValidators = false
	   
	   ordinals.foreach(ord => {
		   val  key = "validator." + ord
		   val validatorTagList = getOptionalStringListParam(appConfig, key)
		   validatorTagList match {
		     case Some(li:java.util.List[String]) => {
		    	 val valTags = BasicUtils.fromListToStringArray(li)
		    	 createValidators(appConfig, valTags, ord, validationSchema, mutValidators)
		    	 foundPropConfigBasedValidators = true
		     }
		     case None =>
		   }  		     
	   })
	   
	   //complex validators
	   var foundSchemaBasedValidators = false
	   if (!foundPropConfigBasedValidators) {
	      validationSchema.getAttributes().asScala.foreach( attr  => {
	    	  	if (null != attr.getValidators()) {
	    	  		val validatorTags =  attr.getValidators().asScala.toArray
	    	  		createValidators(appConfig, validatorTags, attr.getOrdinal(), validationSchema, mutValidators)
	    	  		foundSchemaBasedValidators = true
	    	  	}
	      })
	   }
	   
	   //custom validators
	   if (!foundPropConfigBasedValidators && !foundSchemaBasedValidators) {
         val bulderClassName = getMandatoryStringParam(appConfig, "validatorBuilderClass", 
             "missing validator builder class name")
         val obj = Class.forName(bulderClassName).newInstance()
         val builder = obj.asInstanceOf[ValidatorBuilder]
         builder.createValidators(appConfig, this)
	   }
	   
	   if (debugOn) {
	     validators.foreach(kv => {
	       println("field: " + kv._1 + " num validators:" + kv._2.length )
	     })
	   }
	   
	   //broadcast validator
	   val brValidators = sparkCntxt.broadcast(validators)

	  val data = sparkCntxt.textFile(inputPath)

	   //apply validators to each field in each line to create RDD of tagged records
	   val taggedData = data.map(line => {
	     val items = line.split(fieldDelimIn, -1)
	     val itemsZipped = items.zipWithIndex
	    
	     //apply all validators for the field
	     val valMap = brValidators.value
	     val taggedItems = itemsZipped.map(z => {
	    	val valListOpt = valMap.get(z._2)
	    	
	    	valListOpt match  {
	    	  case Some(valList: Array[Validator]) => {
	    		  val valStatuses = valList.map(validator => {
	    		  val status = validator.isValid(z._1)
	    		  (validator.getTag(), status)
	    		  })
	    		  
	    		  if (debugOn) {
	    		    println("field: " + z._1)
	    		    valStatuses.foreach(vs => println("validator: " + vs._1 + " status:" + vs._2))
	    		  }
	    	
	    		  //only failed validators
	    		  val failedValidators = valStatuses.filter(s => {
	    		   !s._2
	    		  }).map(vs => vs._1)
	    
	    		  val field = 
	    	       	if (failedValidators.isEmpty)
	    	    	   z._1
	    	    	 else 
	    			   z._1 + valTagSeparator + failedValidators.mkString(fieldDelimOut)
	    	    field
	    	  }
	    	  
	    	  case None =>  z._1
	    	}
	    })
	 
	    taggedItems.mkString(fieldDelimOut)
	  })
	 taggedData.cache

	 //filter valid data
	 if (filterInvalidRecords) {
	     //only valid records
		 val validData = taggedData.filter(line => !line.contains(valTagSeparator))
		 validData.saveAsTextFile(outputPath)
	 } else {
	   //all records
	   taggedData.saveAsTextFile(outputPath)
	 }
	  
	 //output invalid data
	 if (outputInvalidRecords){
		val invalidData = taggedData.filter(line => line.contains(valTagSeparator))
		invalidRecordsOutputFile match {
		  case Some(path:String) => invalidData.saveAsTextFile(path)
		  case None =>
		}
	 }
   }
   
   /**
   * @param config
   * @param valTags
   * @param ord
   * @param validatorConfig
   * @param validationSchema
   */
   private  def createValidators( config : Config , valTags : Array[String],   ord : Int,
       validationSchema :  ProcessorAttributeSchema, mutValidators : scala.collection.mutable.HashMap[Int, Array[Validator]])  {
	   val validatorList =  List[Validator]()
	   val  prAttr = validationSchema.findAttributeByOrdinal(ord)
	   val validatorConfig = config.atPath("app")
	   val validators = valTags.map(tag => {
		    val validator = tag match {
		     case "zscoreBasedRange" => {
		    	 getAttributeStats(config.getString("stats.file.path"))
		    	 ValidatorFactory.create(tag, prAttr, validationContext)
		     }
		     
		     case "robustZscoreBasedRange" => {
		    	 getAttributeMeds(config.getString("med.stats.file.path"), config.getString("mad.stats.file.path"), 
		    			 BasicUtils.intArrayFromString(config.getString("id.ordinals"), ",") )		       
		    	 ValidatorFactory.create(tag, prAttr,validationContext)
		     }
		    
		     case tag:String => {
		       ValidatorFactory.create(tag, prAttr,  validatorConfig)
		     }
		   }
		   validator 
	   })
	   
	   //add validators to map
	   mutValidators += ord -> validators
   }

  /**
 * @param statsFilePath
 * @return
 */
  private def getAttributeStats(statsFilePath : String) {
    statsManager = statsManager match{
     	case None => Some( new NumericalAttrStatsManager(statsFilePath, ",", true))
     	case Some(s) => statsManager
    }
    
    //validationContext.
     validationContext.clear()
    validationContext.put("stats",  statsManager.get)
  }
   
  private def getAttributeMeds(medFilePath : String, madFilePath:String, idOrdinals : Array[Int] ) {
    medStatManager = medStatManager match{
     	case None => Some(new MedianStatsManager(medFilePath, madFilePath,  
        			",",  idOrdinals))
     	case Some(s) => medStatManager
    }
    
    //validationContext.
    validationContext.clear()
    validationContext.put("stats",  medStatManager.get)
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
