#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash.local:7077

case "$1" in

"simpleValidator")
	echo "running SimpleDataValidator"
	CLASS_NAME=org.chombo.spark.etl.SimpleDataValidator
	INPUT=hdfs:///etl/input/retail.txt
	OUTPUT=hdfs:///etl/output
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"validator")
	echo "running DataValidator"
	CLASS_NAME=org.chombo.spark.etl.DataValidator
	INPUT=hdfs:///input/etl/val/elec_prod.txt
	OUTPUT=hdfs:///output/etl/val/main
	OUTPUT_INVALID=hdfs:///output/etl/val/inva
	hdfs dfs -rm -r $OUTPUT
	hdfs dfs -rm -r $OUTPUT_INVALID
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"jsonExtractor")
	echo "running FlatRecordExtractorFromJson"
	CLASS_NAME=org.chombo.spark.etl.FlatRecordExtractorFromJson
	INPUT=hdfs:///etl/input/jex/usage.json
	OUTPUT=hdfs:///etl/output/jex
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac