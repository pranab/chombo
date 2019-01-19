#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
MASTER=spark://akash.local:7077

case "$1" in

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/teg/*
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/mea
	rm -rf ./output/mea
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cen.conf
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	DDIR="$2"
	echo "destination directory $PROJECT_HOME/bin/chombo/other/$DDIR/"
	rm $PROJECT_HOME/bin/chombo/output/mea/_SUCCESS
	> $PROJECT_HOME/bin/chombo/other/$DDIR/stats.txt
	for filename in $PROJECT_HOME/bin/chombo/output/mea/*; do
	  echo "copying  $filename"
	  cat $filename >> $PROJECT_HOME/bin/chombo/other/$DDIR/stats.txt
	done
	echo "copied file"
	ls -l $PROJECT_HOME/bin/chombo/other/$DDIR
;;

"tempAggr")
	echo "running TemporalAggregator Spark job"
	CLASS_NAME=org.chombo.spark.explore.TemporalAggregator
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/teg/cusage.csv
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/teg
	rm -rf ./output/teg
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cen.conf
;;

"crAucInput")
	echo "copying and consolidating tem aggregation output file"
	cat $PROJECT_HOME/bin/chombo/output/teg/part-00000 > $PROJECT_HOME/bin/chombo/input/auc/cusage.csv
	cat $PROJECT_HOME/bin/chombo/output/teg/part-00001 >> $PROJECT_HOME/bin/chombo/input/auc/cusage.csv
	ls -l $PROJECT_HOME/bin/chombo/input/auc
;;

"autoCor")
	echo "running AutoCorrelation Spark job"
	CLASS_NAME=org.chombo.spark.explore.AutoCorrelation
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/auc/cusage.csv
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/auc
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cen.conf
;;

"numDistrStat")
	echo "running NumericalAttrDistrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrDistrStats
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/csf/*
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/csf
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cen.conf
;;

"statsFilter")
	echo "running StatsBasedFilter Spark job"
	CLASS_NAME=org.chombo.spark.etl.StatsBasedFilter
	INPUT=file:///Users/pranab/Projects/bin/chombo/input/teg/cusage.csv
	OUTPUT=file:///Users/pranab/Projects/bin/chombo/output/sfi
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cen.conf
;;

"cpFiltFile")
	echo "copying filtered data"
	DDIR="$2"
	rm $PROJECT_HOME/bin/chombo/output/sfi/_SUCCESS
	rm $PROJECT_HOME/bin/chombo/input/$DDIR/*
	echo "destination directory $PROJECT_HOME/bin/chombo/input/$DDIR/"
	for filename in $PROJECT_HOME/bin/chombo/output/sfi/*; do
	  echo "copying  $filename"
	  cp $filename $PROJECT_HOME/bin/chombo/input/$DDIR/
	done
	echo "copied files"
	ls -l $PROJECT_HOME/bin/chombo/input/$DDIR/
;;


*) 
	echo "unknown operation $1"
	;;

esac