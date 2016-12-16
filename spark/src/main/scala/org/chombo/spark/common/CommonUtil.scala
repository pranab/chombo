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

import scala.reflect.ClassTag

/**
 * @author pranab
 *
 */
object CommonUtil {

  /**
  * @param obj
  * @return
  */
  def asOption[T:ClassTag](obj : T) : Option[T] = {
    if (null != obj) {
      Some(obj)
    } else {
      None
    }
  }
  
  /**
  * @param arr
  * @return
  */
  def filterNoneFromArray[T:ClassTag](arr : Array[Option[T]]) : Array[T] = {
      //remove None
	  val filtArr = arr.filter(a => {
	    a match {
	      case Some(x:T) => true
	      case None => false
	    }
	  })
		  
	  //peel off Some
	  val rawArr = filtArr.map(a => {
	    a match {
	      case Some(x:T) => x
	      case None => throw new IllegalStateException("no none exccepted")
	    }
	    
	  })
	rawArr 
  }
  
  /**
  * @param name
  * @return
  */
  def createInstance[T:ClassTag](name: String): T = {
    val obj = Class.forName(name).newInstance()
	obj.asInstanceOf[T]
  }  
}