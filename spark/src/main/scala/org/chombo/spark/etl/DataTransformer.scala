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
import org.chombo.util.BasicUtils
import org.chombo.transformer.TransformerFactory
import com.typesafe.config.Config
import org.chombo.util.ProcessorAttributeSchema
import org.chombo.transformer.AttributeTransformer

object DataTransformer extends JobConfiguration  {
    val mutTransformers : scala.collection.mutable.HashMap[Int, Array[AttributeTransformer]]   = scala.collection.mutable.HashMap()
    lazy val transformers :  Map[Int, Array[AttributeTransformer]]   = mutTransformers.toMap
    var generators : Array[Option[AttributeTransformer]] = _
    
    /**
    * @param args
    * @return
    */
    def main(args: Array[String]) {
       val appName = "dataTransformer"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val transformerSchema = BasicUtils.getProcessingSchema(
	       getMandatoryStringParam(appConfig, "schema.file.path", "missing schema file path configuration"))
	   transformerSchema.validateTargetAttributeMapping()
	   val customClass = if (appConfig.hasPath("custom.trans.factory.class"))
	     appConfig.getString("custom.trans.factory.class")
	   else
	     null
	   TransformerFactory.initialize(customClass, appConfig)
	   val ordinals  = transformerSchema.getAttributeOrdinals()
	   
	   var foundinConfig = false
	   ordinals.foreach(ord => {
		   val  key = "transformer." + ord
		   val transformerTagList = getOptionalStringListParam(appConfig, key)
		   transformerTagList match {
		     case Some(li:java.util.List[String]) => {
		    	 val transTags = BasicUtils.fromListToStringArray(li)
		    	 createTransformers(transformerSchema, appConfig, transTags,  ord)
		    	 foundinConfig = true
		     }
		     case None =>
		   }  
	   })
	   
	   if (foundinConfig) {
		   generators = ordinals.map(ord => {
			   val  key = "generator." + ord
			   val generatorTag = getOptionalStringParam(appConfig, key)
			   generatorTag match {
			     case Some(genTag:String) => {
			   		Some(createGenerator(transformerSchema, appConfig, genTag,  ord))
			   	  }
			   	  case None => None
			   }  
		   })
	   }
	   
       
    }
   
    /**
	* @param appName
	* @param config
	* @param includeAppConfig
	* @return
	*/    
    private  def createTransformers(transformerSchema : ProcessorAttributeSchema, config : Config , 
       transTags : Array[String],   ord : Int) {
        val prAttr = transformerSchema.findAttributeByOrdinal(ord)
         val transArr = transTags.map(tag => {
          TransformerFactory.createTransformer(tag, prAttr, config)
        })
        //add transformers to map
	    mutTransformers += ord -> transArr
   }

    /**
	* @param appName
	* @param config
	* @param includeAppConfig
	* @return
	*/    
    private  def createGenerator(transformerSchema : ProcessorAttributeSchema, config : Config , 
       genTag : String, ord : Int) : AttributeTransformer = {
         val prAttr = transformerSchema.findAttributeByOrdinal(ord)
         TransformerFactory.createTransformer(genTag, prAttr, config)
   }
}