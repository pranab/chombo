#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"numStat")
	echo "running NumericalAttrDistrStats"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrDistrStats
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/nad/conv2.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/nad
	rm -rf ./output/nad
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT exp.conf
;;

"dataTypeInferencer")
	echo "running DataTypeInferencer"
	CLASS_NAME=org.chombo.spark.explore.DataTypeInferencer
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/dti/sell_trans.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/dti
	rm -rf ./output/dti
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT exp.conf
;;

"secSort")
	echo "running SecondarySorting"
	CLASS_NAME=org.chombo.spark.sanity.SecondarySorting
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/ses/cusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/ses
	rm -rf ./output/ses
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT exp.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac