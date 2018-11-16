#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"crBaseFile")
	./prod_inv.py ini $2 $3 $4 > $5
	ls -l $5
;;

"crBaseDcFile")
	./prod_inv.py din $2 > $3
	ls -l $3
;;

"crUpsert")
	./prod_inv.py upi $2 $3 $4 > $5
	ls -l $5
	echo "base file before mutation"
	ls -l $2
	cp $2 old_$2
	cat $3 > $2
	cat $5 >> $2
	echo "base file after mutation"
	ls -l $2
;;

"crDel")
	./prod_inv.py del  $2 $3 > $4
	ls -l $4
	echo "before mutation"
	ls -l $2
	cp $2 old_$2
	cat $4 > $2
	echo "after mutation"
	ls -l $2
;;

"rmInput")
	hdfs dfs -rm /input/bmu/*
	echo "** input dir"
	hdfs dfs -ls  /input/bmu
;;

"cpInput")
	hdfs dfs -put $2 /input/bmu
	echo "** input dir"
	hdfs dfs -ls /input/bmu
;;

"rmIncInput")
	hdfs dfs -rm /other/bmu/inc/*
	echo "** inc dir"
	hdfs dfs -ls  /other/bmu/inc
;;

"cpIncInput")
	hdfs dfs -put $2 /other/bmu/inc
	echo "** inc dir"
	hdfs dfs -ls /other/bmu/inc
;;

"bulkMutator")
	echo "running RecordSetBulkMutator"
	CLASS_NAME=org.chombo.spark.etl.RecordSetBulkMutator
	INPUT=hdfs:///input/bmu/*
	OUTPUT=hdfs:///output/bmu
	hdfs dfs -rm -r $OUTPUT
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $JAR_NAME  $INPUT $OUTPUT etl.conf
;;

"mvOutput")
	hdfs dfs -rm /output/bmu/_SUCCESS
	hdfs dfs -rm -r /backup/bmu
	hdfs dfs -mv /input/bmu /backup
	echo "** backup dir"
	hdfs dfs -ls /backup
	echo "** input dir"
	hdfs dfs -ls /input
	hdfs dfs -mv /output/bmu /input
	echo "** input dir after mv"
	hdfs dfs -ls /input/bmu
;;

*) 
	echo "unknown operation $1"
	;;

esac