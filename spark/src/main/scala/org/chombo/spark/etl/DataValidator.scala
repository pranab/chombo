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
import org.chombo.util.Utility
import org.chombo.validator.ValidatorFactory
import com.typesafe.config.Config
import com.typesafe.config.ConfigValue
import org.chombo.validator.Validator
import org.chombo.util.ProcessorAttributeSchema
import org.chombo.util.NumericalAttrStatsManager
import org.chombo.util.MedianStatsManager
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
@author pranab
 *
* */
object DataValidator extends JobConfiguration  {
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
	   val Array( inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val localConfig = config.atPath("app")
	   val sparkConf = createSparkConf("app.data validation", config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   
	   if (localConfig.hasPath("app.invalid.records.output.file"))
	     localConfig.getString("app.invalid.records.output.file")

	   val fieldDelimIn = localConfig.getString("app.field.delim.in")
	   val fieldDelimOut = localConfig.getString("app.field.delim.out")
	   val valTagSeparator = localConfig.getString("app.val.tag.separator")
	   val filterInvalidRecords = localConfig.getBoolean("app.filter.invalid.records")
	   val outputInvalidRecords = localConfig.getBoolean("app.output.invalid.records")
	   val invalidRecordsOutputFile = 
	   if (localConfig.hasPath("app.invalid.records.output.file"))
	     localConfig.getString("app.invalid.records.output.file")
	   else 
		""
	   val validationSchema = Utility.getProcessingSchema( localConfig.getString("app.schema.file.path"))
	   
	   val validatorConfig = config.atPath("app")

	   val configClass =
	   if (localConfig.hasPath("app.custom.valid.factory.class")) 
		localConfig.getString("app.custom.valid.factory.class") 
	   else 
		 null
	   ValidatorFactory.initialize(configClass, validatorConfig )
	   val ordinals =  validationSchema.getAttributeOrdinals()
	   val tagSep = localConfig.getString( "app.val.tag.separator")
	   
	   //initialize stats manager
	   if(localConfig.hasPath("app.stats.file.path"))
	      getAttributeStats(localConfig.getString("app.stats.file.path"))
	   if(localConfig.hasPath("app.med.stats.file.path"))
 	      getAttributeMeds(localConfig.getString("app.med.stats.file.path"), localConfig.getString("app.mad.stats.file.path"), 
	           Utility.intArrayFromString(localConfig.getString("app.id.ordinals"), ",") )
	  

	   //simple validators  
	   var foundSimpleValidators = false

	   ordinals.foreach(ord => {
		   val  key = "app.validator." + ord
		   if (localConfig.hasPath(key)) {
			   val validatorTag : String = localConfig.getString(key)
			   val valTags :Array[String] = validatorTag.split(tagSep);
			   createValidators(config, valTags, ord, validationSchema, mutValidators)
			   foundSimpleValidators = true
		   }
	   })
	   
	   //complex validators
	   if (!foundSimpleValidators) {
	      validationSchema.getAttributes().asScala.foreach( attr  => {
	    	  	if (null != attr.getValidators()) {
	    	  		val validatorTags =  attr.getValidators().asScala.toArray
	    	  		createValidators(config, validatorTags, attr.getOrdinal(), validationSchema, mutValidators)
	    	  	}
	      })
	   }
	   
	  val data = sparkCntxt.textFile(inputPath)
	  
	  //apply validators to each field in each line to create RDD of tagged records
	  val taggedData = data.map(line => {
	    val items = line.split(fieldDelimIn)
	    val itemsZipped = items.zipWithIndex
	    
	    //apply all validators for the field
	    val taggedItems = itemsZipped.map(z => {
		println("The value of z is " + z)
	    	val valList : Array[Validator] = validators.get(z._2).get
	    	val valStatuses = valList.map(validator => {
	    		val status = validator.isValid(z._1)
	    		(validator.getTag(), status)
	    	})
		
	    	//only failed validators
	    	val failedValidators = valStatuses.filter(s => {
	    	  !s._2
	    	}).map(vs => vs._1)
	    
	    	val field = if (failedValidators.isEmpty)
	    		z._1
	    	else 
	    	  z._1 + valTagSeparator  + failedValidators.mkString(fieldDelimOut)
	    	  
	    	field
	    })
	    taggedItems.mkString(fieldDelimOut)
	  })
	 taggedData.cache
	 
	 //filter valid data
	 val validData = taggedData.filter(line => !line.contains(":"))
	 val list = validData.collect()
	 list.foreach(println)
	 validData.saveAsTextFile(outputPath)
	  
	 //filter invalid data
	 if (outputInvalidRecords){
		val invalidData = taggedData.filter(line => line.contains(valTagSeparator))
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
private  def createValidators( config : Config , valTags : Array[String],   ord : Int,
       validationSchema :  ProcessorAttributeSchema, mutValidators : scala.collection.mutable.HashMap[Int, Array[Validator]])  {
	   val validatorList =  List[Validator]()
	   val prAttr = validationSchema.findAttributeByOrdinal(ord)
	   val validatorConfig = config
	   val validators = valTags.filter(_.length > 0 ).map(tag => {
		    val validator = tag match {
		     case "zscoreBasedRange" => {
		    	 getAttributeStats(config.getString("app.stats.file.path"))
		    	 ValidatorFactory.create(tag, prAttr, validationContext)
		     }
		     
		     case "robustZscoreBasedRange" => {
		    	 getAttributeMeds(config.getString("app.med.stats.file.path"), config.getString("app.mad.stats.file.path"), 
		    			 Utility.intArrayFromString(config.getString("app.id.ordinals"), ",") )		       
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
     	case None => Some(  new MedianStatsManager(medFilePath, madFilePath,  
        			",",  idOrdinals))
     	case Some(s) => medStatManager
    }
    
    //validationContext.
    validationContext.clear()
    validationContext.put("stats",  medStatManager.get)
  }
}
