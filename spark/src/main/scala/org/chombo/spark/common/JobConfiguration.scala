/*
 * chombo: on spark
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


package org.chombo.spark.common

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import collection.JavaConversions._
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils

/**
 * Various configuration helper methods for spark jobs
 * @author pranab
 *
 */
trait JobConfiguration {

  /**
   * @param args
   * @return
   */
   def configFileFromCommandLine(args: Array[String]) : String = {
		val confifFilePath = args.length match {
			case x: Int if x == 1 => args(0)
			case _ => throw new IllegalArgumentException("invalid number of  command line args, expecting 1")
		}
		confifFilePath
    }

	/**
	 * @param args
	 * @param numArgs
	 * @return
	 */
	def getCommandLineArgs(args: Array[String], numArgs : Int) : Array[String] = {
		val argArray = args.length match {
			case x: Int if x == numArgs => args.take(numArgs)
			case _ => throw new IllegalArgumentException("invalid number of  command line args, expecting " + numArgs)
		}
	    argArray
	}

	/**
	 * @param args
	 * @return
	 */
	def getCommandLineArgs(args: Array[String]) : Array[String] = {
		val argArray = args.length match {
			case x: Int if x == 4 => args.take(4)
			case _ => throw new IllegalArgumentException("invalid number of  command line args, expecting 4")
		}
	    argArray
	}
	
	/**
	 * @param configFile
	 * @return
	 */
	def createConfig(configFile : String) : Config = {
		System.setProperty("config.file", configFile)
		ConfigFactory.load()
	}
	
	/**
	 * @param master
	 * @param appName
	 * @param executorMemory
	 * @return
	 */
	def createSparkConf(master : String, appName : String, executorMemory : String = "1g") : SparkConf =  {
	  new SparkConf()
		.setMaster(master)
		.setAppName(appName)
		.set("spark.executor.memory", executorMemory)
	}
	
	/**
	 * @param appName
	 * @param config
	 * @return
	 */
	def createSparkConf(appName : String, config : Config, includeAppConfig: Boolean) : SparkConf =  {
		val sparkConf = new SparkConf()
		.setAppName(appName)
		
		if(config.hasPath("system.master")) {
			val master = config.getString("system.master")
			sparkConf.setMaster(master)
		}
		
		//all spark properties
		if (config.hasPath("sparkParam")) {
			val sparkList = config.getConfigList("sparkParam").toList
			sparkList.map ( cfg => {
				val name = cfg.getString("name")
				val value = cfg.getString("value")
				sparkConf.set(name, value)
		  })
		}
	  
		//all app properties
		if (includeAppConfig && config.hasPath(appName)) {
			val appList = config.getConfigList(appName).toList
			appList.map ( cfg => {
				  val name = "app." + cfg.getString("name")
				  val value = cfg.getString("value")
				  sparkConf.set(name, value)
			  })
		}
	  
		sparkConf
	}

	/**
	 * @param sparkCntxt
	 * @param config
	 * @param paramNames
	 */
	def addJars(sparkCntxt : SparkContext, config : Config, paramNames : String*) {
	  paramNames.foreach(param => {  
	    sparkCntxt.addJar(config.getString(param))
	  })
	}
	
	/**
	 * @param sparkCntxt
	 * @param config
	 * @param fromList
	 * @param paramName
	 */
	def addJars(sparkCntxt : SparkContext, config : Config, fromList : Boolean, paramName : String) {
	  if (config.hasPath(paramName)) {
	  	val jarPaths = config.getStringList(paramName).toList
	  	jarPaths.foreach(jar => {  
	  		sparkCntxt.addJar(jar)
	  	})
	  }
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryStringParam(config:Config, paramName:String, errorMsg:String) : String = {
	  getStringParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryStringParam(config:Config, paramName:String) : String = {
	  getStringParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getStringParamOrElse(config:Config, paramName:String, defValue:String) : String = {
	  getStringParam(config, paramName, Some(defValue), None)
	}
	
	/**
	 * condition
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getConditionalMandatoryStringParam(condition:Boolean, config:Config, paramName:String, errorMsg:String) : String = {
      if (condition) getMandatoryStringParam(config, paramName, errorMsg) else null
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getStringParam(config:Config, paramName:String, defValue:Option[String], errorMsg:Option[String]) : String = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getString(paramName)
	  } else {
	    defValue match {
	      case Some(va:String) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryIntParam(config:Config, paramName:String, errorMsg:String) : Int = {
	  getIntParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryIntParam(config:Config, paramName:String) : Int = {
	  getIntParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getIntParamOrElse(config:Config, paramName:String, defValue:Int) : Int = {
	  getIntParam(config, paramName, Some(defValue), None)
	}

	/**
	 * condition
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getConditionalMandatoryIntParam(condition:Boolean, config:Config, paramName:String, errorMsg:String) : Int = {
      if (condition) getMandatoryIntParam(config, paramName, errorMsg) else 0
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getIntParam(config:Config, paramName:String, defValue:Option[Int], errorMsg:Option[String]) : Int = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getInt(paramName)
	  } else {
	    defValue match {
	      case Some(va:Int) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryDoubleParam(config:Config, paramName:String, errorMsg:String) : Double = {
	  getDoubleParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryDoubleParam(config:Config, paramName:String) : Double = {
	  getDoubleParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getDoubleParamOrElse(config:Config, paramName:String, defValue:Double) : Double = {
	  getDoubleParam(config, paramName, Some(defValue), None)
	}
	
	/**
	 * condition
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getConditionalMandatoryDoubleParam(condition:Boolean, config:Config, paramName:String, errorMsg:String) : Double = {
      if (condition) getMandatoryDoubleParam(config, paramName, errorMsg) else 0
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getDoubleParam(config:Config, paramName:String, defValue:Option[Double], errorMsg:Option[String]) : Double = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getDouble(paramName)
	  } else {
	    defValue match {
	      case Some(va:Double) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryBooleanParam(config:Config, paramName:String, errorMsg:String) : Boolean = {
	  getBooleanParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryBooleanParam(config:Config, paramName:String) : Boolean = {
	  getBooleanParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getBooleanParamOrElse(config:Config, paramName:String, defValue:Boolean) : Boolean = {
	  getBooleanParam(config, paramName, Some(defValue), None)
	}
	
	/**
	 * condition
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getConditionalMandatoryBooleanParam(condition:Boolean, config:Config, paramName:String, errorMsg:String) : Boolean = {
      if (condition) getMandatoryBooleanParam(config, paramName, errorMsg) else false
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getBooleanParam(config:Config, paramName:String, defValue:Option[Boolean], errorMsg:Option[String]) : Boolean = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getBoolean(paramName)
	  } else {
	    defValue match {
	      case Some(va:Boolean) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryStringListParam(config:Config, paramName:String, errorMsg:String) : java.util.List[String] = {
	  getStringListParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryStringListParam(config:Config, paramName:String) : java.util.List[String] = {
	  getStringListParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getStringListParamOrElse(config:Config, paramName:String, defValue:java.util.List[String]) : java.util.List[String] = {
	  getStringListParam(config, paramName, Some(defValue), None)
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getStringListParam(config:Config, paramName:String, defValue:Option[java.util.List[String]], errorMsg:Option[String]) : 
	  java.util.List[String] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getStringList(paramName)
	  } else {
	    defValue match {
	      case Some(va:java.util.List[String]) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryIntListParam(config:Config, paramName:String, errorMsg:String) : java.util.List[Integer] = {
	  getIntListParam(config, paramName, None, Some(errorMsg))
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryIntListParam(config:Config, paramName:String) : java.util.List[Integer] = {
	  getIntListParam(config, paramName, None, None)
	}

	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getIntListParamOrElse(config:Config, paramName:String, defValue:java.util.List[Integer]) : java.util.List[Integer] = {
	  getIntListParam(config, paramName, Some(defValue), None)
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getIntListParam(config:Config, paramName:String, defValue:Option[java.util.List[Integer]], errorMsg:Option[String]) : 
	  java.util.List[Integer] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getIntList(paramName)
	  } else {
	    defValue match {
	      case Some(va:java.util.List[Integer]) => va
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryDoubleListParam(config:Config, paramName:String, errorMsg:String) : java.util.List[java.lang.Double] = {
	  getDoubleListParam(config, paramName, None, Some(errorMsg))
	}
	

	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getMandatoryDoubleListParam(config:Config, paramName:String) : java.util.List[java.lang.Double] = {
	  getDoubleListParam(config, paramName, None, None)
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getDoubleListParamOrElse(config:Config, paramName:String, defValue:java.util.List[Double]) : java.util.List[java.lang.Double] = {
	  getDoubleListParam(config, paramName, Some(defValue), None)
	}

	
	/**
	 * @param config
	 * @param paramName
	 * @param defValue
	 * @param errorMsg
	 * @return
	 */
	def getDoubleListParam(config:Config, paramName:String, defValue:Option[java.util.List[Double]], errorMsg:Option[String]) : 
	  java.util.List[java.lang.Double] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    config.getDoubleList(paramName)
	  } else {
	    defValue match {
	      case Some(va:java.util.List[Double]) => {
	        val dList = new java.util.ArrayList[java.lang.Double]()
	        va.foreach(v => dList.add(v))
	        dList
	      }
	      case None => {
	        errorMsg match {
	          case Some(e:String) => throw new IllegalStateException(e + " parameter: " + paramName)
	          case None => throw new IllegalStateException("missing mandatory configuration parameter: " + paramName)
	        }
	      }
	    }
	  }
	  paramValue
	}
	
	
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalStringParam(config:Config, paramName:String) : Option[String] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getString(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalIntParam(config:Config, paramName:String) : Option[Int] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getInt(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalDoubleParam(config:Config, paramName:String) : Option[Double] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getDouble(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalBooleanParam(config:Config, paramName:String) : Option[Boolean] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getBoolean(paramName))
	  } else {
	    None
	  }
	  paramValue
	}

	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalStringListParam(config:Config, paramName:String) : Option[java.util.List[String]] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getStringList(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalIntListParam(config:Config, paramName:String) : Option[java.util.List[Integer]] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getIntList(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @return
	 */
	def getOptionalDoubleListParam(config:Config, paramName:String) : Option[java.util.List[java.lang.Double]] = {
	  val paramValue = if (config.hasPath(paramName)) {
	    Some(config.getDoubleList(paramName))
	  } else {
	    None
	  }
	  paramValue
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryStringDoubleMapParam(config:Config, paramName:String, errorMsg:String) : java.util.Map[String, Double] = {
	  val paramMap = new java.util.HashMap[String, Double]
	  val paramList = getMandatoryStringListParam(config, paramName, errorMsg)
	  for (param <- paramList) {
	    val items = param.split(BasicUtils.DEF_SUB_FIELD_DELIM)
	    paramMap.put(items(0), items(1).toDouble)
	  }
	  paramMap
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryStringIntMapParam(config:Config, paramName:String, errorMsg:String) : java.util.Map[String, Int] = {
	  val paramMap = new java.util.HashMap[String, Int]
	  val paramList = getMandatoryStringListParam(config, paramName, errorMsg)
	  for (param <- paramList) {
	    val items = param.split(BasicUtils.DEF_SUB_FIELD_DELIM)
	    paramMap.put(items(0), items(1).toInt)
	  }
	  paramMap
	}
	
	/**
	 * @param config
	 * @param paramName
	 * @param errorMsg
	 * @return
	 */
	def getMandatoryIntDoubleMapParam(config:Config, paramName:String, errorMsg:String) : java.util.Map[Int, Double] = {
	  val paramMap = new java.util.HashMap[Int, Double]
	  val paramList = getMandatoryStringListParam(config, paramName, errorMsg)
	  for (param <- paramList) {
	    val items = param.split(BasicUtils.DEF_SUB_FIELD_DELIM)
	    paramMap.put(items(0).toInt, items(1).toDouble)
	  }
	  paramMap
	}
	
}