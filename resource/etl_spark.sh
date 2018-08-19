#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

#"simpleValidator")
#	echo "running SimpleDataValidator"
#	CLASS_NAME=org.chombo.spark.etl.SimpleDataValidator
#	INPUT=hdfs:///etl/input/retail.txt
#	OUTPUT=hdfs:///etl/output
#	hdfs dfs -rm -r $OUTPUT
#	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
#	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
#;;

"validator")
	echo "running DataValidator"
	CLASS_NAME=org.chombo.spark.etl.DataValidator
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/etl/val/retail_orders.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/etl/val/main
	rm -rf ./output/etl/val/main
	rm -rf ./output/etl/val/inva
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"jsonExtractor")
	echo "running FlatRecordExtractorFromJson"
	CLASS_NAME=org.chombo.spark.etl.FlatRecordExtractorFromJson
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/jex/usage.json
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/jex
	rm -rf ./output/jex
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"cmplxJsonExtractor")
	echo "running FlatRecordExtractorFromJson"
	CLASS_NAME=org.chombo.spark.etl.FlatRecordExtractorFromJson
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/jex/smEvents.json
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/jex
	rm -rf ./output/jex
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"normalizer")
	echo "running Normalizer"
	CLASS_NAME=org.chombo.spark.etl.Normalizer
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/norm/price.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/norm
	rm -rf ./output/norm
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"simpleValidator")
	echo "running SimpleDataValidator"
	CLASS_NAME=org.chombo.spark.etl.SimpleDataValidator
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/siva/order.txt
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/siva
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
	rm -rf ./output/siva/_SUCCESS
;;


"dupRemover")
	echo "running DuplicateRemover"
	CLASS_NAME=org.chombo.spark.etl.DuplicateRemover
	INPUT=file:///Users/pranab/Projects/bin/chombo/output/siva
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/dupr
	rm -rf ./output/dupr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;



*) 
	echo "unknown operation $1"
	;;

esac