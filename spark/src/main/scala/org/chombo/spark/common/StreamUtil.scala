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

import org.apache.spark.streaming.dstream.DStream
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.collection.mutable.Buffer
import scala.collection.JavaConverters._

object StreamUtil {
	/**
	 * Get stream for various sources
	 * @param config
	 * @param strContxt
	 * @return
	 */
	def getStreamSource(config: Config, strContxt: StreamingContext) : DStream[String] = {
	  	val source = config.getString("general.stream.source")
	  	val strm = source match {
			//HDFS files as stream source
	  		case "hdfs" => {
	  			val path = config.getString("source.hdfs.path")
	  			strContxt.textFileStream(path)
	  		}
	  
	  		//socket server as stream source
	  		case "socketText" => {
	  			val host = config.getString("source.socket.receiver.host")
	  			val port = config.getInt("source.socket.receiver.port")
	  			strContxt.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER_2)
	  		}
	  		
	  		case "kafka" => {
	  			//kafka as stream source 
	  			val brokerList = config.getString("source.metadata.broker.list")
	  			val topic = config.getString("source.kafka.topic")
	    
	  			val kafkaParams: Map[String, String] = Map(
	  						  "metadata.broker.list" -> brokerList
	  			)
	  			val topics = Set(topic)
	  			val st = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
	  				strContxt, kafkaParams, topics)
	  			st.map(r => r._2)
	 	  	}

	  	}
	  	strm
	}
	
	/**
	 * @param config
	 * @param strContxt
	 * @param fieldOrdinals
	 * @param fieldDelimIn
	 * @return
	 */
	def getKeyedStreamSource(config: Config, strContxt: StreamingContext) : DStream[(Record, String)] = {
	  	val source = config.getString("general.stream.source")
	  	val keyFieldOrdinals = config.getIntList("field.key.ordinals").asScala
	  	val fieldDelimIn = config.getString("field.delim.in")
	  	val debugOn = config.getBoolean("general.debug.on")
	  	
	  	val strm = source match {
			//HDFS files as stream source
	  		case "hdfs" => {
	  			val path = config.getString("source.hdfs.path")
	  			val st = strContxt.textFileStream(path)
	  			st.map(r => {
	  			  val fields = r.split(fieldDelimIn)
	  			  val id = Record.extractFields(fields, keyFieldOrdinals)
	  			  (id, r)
	  			})
	  			
	  		}
	  
	  		//socket server as stream source
	  		case "socketText" => {
	  			val host = config.getString("source.socket.receiver.host")
	  			val port = config.getInt("source.socket.receiver.port")
	  			val st = strContxt.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER_2)
	  			if (debugOn) {
	  			  st.foreach(rdd => {println("*** num of records in RDD: " + rdd.count)})
	  			}
	  			st.map(r => {
	  			  val fields = r.split(fieldDelimIn)
	  			  val id = Record.extractFields(fields, keyFieldOrdinals)
	  			  (id, r)
	  			})
	  		}
	  		
	  		case "kafka" => {
	  			//kafka as stream source 
	  			val brokerList = config.getString("source.metadata.broker.list")
	  			val topic = config.getString("source.kafka.topic")
	    
	  			val kafkaParams: Map[String, String] = Map(
	  						  "metadata.broker.list" -> brokerList
	  			)
	  			val topics = Set(topic)
	  			val st = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
	  				strContxt, kafkaParams, topics)
	  			val extractKey = config.getBoolean("source.kafka.extractKey")
	  			st.map(r => {
	  			  extractKey match {
	  			    case true => {
	  			    	val fields = r._2.split(fieldDelimIn)
	  			    	val id = Record.extractFields(fields, keyFieldOrdinals)
	  			    	(id, r._2)
	  			    }
	  			    case false => {
	  			    	val fields = r._1.split(fieldDelimIn)
	  			    	val id = Record.extractFields(fields, keyFieldOrdinals)
	  			    	(id, r._2)
	  			    }
	  			  }
	  			})
	 	  	}

	  	}
	  	strm
	}
	
}