#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"simpleValidator")
	echo "running simpleValidator"
	CLASS_NAME=org.chombo.spark.etl.SimpleDataValidator
	INPUT=hdfs:///etl/input/retail.txt
	OUTPUT=hdfs:///etl/output
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac