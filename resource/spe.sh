#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash.local:7077

case "$1" in

"cpInp")
	fn="$2"
	rm $PROJECT_HOME/bin/chombo/input/sst/*
	cp $fn $PROJECT_HOME/bin/chombo/input/sst/
	rm $PROJECT_HOME/bin/chombo/input/ssd/*
	cp $fn $PROJECT_HOME/bin/chombo/input/ssd/
	echo "copied files"
	ls -l $PROJECT_HOME/bin/chombo/input/sst/
	ls -l $PROJECT_HOME/bin/chombo/input/sdi/
;;

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/sst/*
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/sst
	rm -rf ./output/mea
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT spe.conf
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	SDIR="$2"
	DDIR="$3"
	DFILE="$4"
	echo "destination directory $PROJECT_HOME/bin/chombo/other/$DDIR/"
	rm $PROJECT_HOME/bin/chombo/output/$SDIR/_SUCCESS
	> $PROJECT_HOME/bin/chombo/other/$DDIR/$DFILE
	for filename in $PROJECT_HOME/bin/chombo/output/$SDIR/*; do
	  echo "copying  $filename"
	  cat $filename >> $PROJECT_HOME/bin/chombo/other/$DDIR/$DFILE
	done
	echo "copied file"
	ls -l $PROJECT_HOME/bin/chombo/other/$DDIR
;;

"numDistrStat")
	echo "running NumericalAttrDistrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrDistrStats
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/sdi/*
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/sdi
	rm -rf ./output/sdi
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT spe.conf
;;


*) 
	echo "unknown operation $1"
	;;

esac