#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash.local:7077

case "$1" in

"numStat")
	echo "running NumericalAttrDistrStats"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrDistrStats
	INPUT=hdfs:///projects/expl/nad/input/call_records.txt
	OUTPUT=hdfs:///projects/expl/nad/output
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT exp.conf
;;


*) 
	echo "unknown operation $1"
	;;

esac