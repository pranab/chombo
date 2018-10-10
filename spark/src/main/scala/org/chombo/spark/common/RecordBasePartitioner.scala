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

import org.apache.spark.Partitioner

/**
 * Partitioning using base part of key for secondary sorting
 * @author pranab
 *
 */
class RecordBasePartitioner(partitions: Int) extends Partitioner {
	override def numPartitions: Int = partitions
  
	/**
	 * @param key
	 * @return
	 */
	override def getPartition(key: Any): Int = {
      val rec = key.asInstanceOf[Record]
      var hCode = rec.baseHashCode()
      hCode % numPartitions
    }
}