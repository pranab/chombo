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
	def createSparkConf(appName : String, config : Config) : SparkConf =  {
	  val sparkConf = new SparkConf()
		.setMaster(config.getString("system.master"))
		.setAppName(appName)
		
		val z = config.getConfigList("spark")
		
		//all spark properties
		config.getConfigList("spark").map ( cfg => {
			  val name = "spark." + cfg.getString("name")
			  val value = cfg.getString("value")
			  sparkConf.set(name, value)
		  }
		)
		
		//all app properties
		if (config.hasPath(appName)) {
			config.getConfigList(appName).map ( cfg => {
				  val name = "app." + cfg.getString("name")
				  val value = cfg.getString("value")
				  sparkConf.set(name, value)
			  }
			)
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
	
}