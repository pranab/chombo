#!/bin/bash
# contains everything needed to execute chombo in batch mode

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
AVENIR_JAR_NAME=/home/pranab/Projects/avenir/target/avenir-1.0.jar
BEYMANI_JAR_NAME=/home/pranab/Projects/beymani/target/beymani-1.0.jar
SIFARISH_JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
HDFS_BASE_DIR=/user/pranab/vaou
PROP_FILE=/home/pranab/Projects/bin/chombo/etl.properties
HDFS_META_BASE_DIR=/user/pranab/meta

case "$1" in

"genOrder")
	 ./store_order.py createOrders $2 $3 $4 > $5
	 ls -l $5
;;

"loadIncr")
	hadoop fs -rm $HDFS_BASE_DIR/ruag/input/$2
	hadoop fs -put $2 $HDFS_BASE_DIR/ruag/input
	hadoop fs -ls $HDFS_BASE_DIR/ruag/input
;;


"runningAggr")
	echo "running MR RunningAggregator"
	CLASS_NAME=org.chombo.mr.RunningAggregator
	IN_PATH=$HDFS_BASE_DIR/ruag/input
	OUT_PATH=$HDFS_BASE_DIR/ruag/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -ls $HDFS_BASE_DIR/ruag/output
;;


"replaceAggr")
	hadoop fs -rm $HDFS_BASE_DIR/ruag/input/part-r-00000
	hadoop fs -mv $HDFS_BASE_DIR/ruag/output/part-r-00000 $HDFS_BASE_DIR/ruag/input
	hadoop fs -ls $HDFS_BASE_DIR/ruag/input
;;

"valOutlier")
	echo "running MR OutlierBasedDataValidation"
	CLASS_NAME=org.chombo.mr.OutlierBasedDataValidation
	IN_PATH=$HDFS_BASE_DIR/ruag/input
	OUT_PATH=$HDFS_BASE_DIR/ouva/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -ls $HDFS_BASE_DIR/ouva/output
;;

"validate")
	echo "running mr ValidationChecker for median"
	CLASS_NAME=org.chombo.mr.ValidationChecker
	IN_PATH=/user/pranab/nuam/input
	OUT_PATH=/user/pranab/nuam/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop fs -rm /user/pranab/output/dava/*
	echo "removed invalid data file"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"median")
	echo "running mr NumericalAttrMedian for median"
	CLASS_NAME=org.chombo.mr.NumericalAttrMedian
	IN_PATH=/user/pranab/nuam/input
	OUT_PATH=/user/pranab/nuam/med/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr /user/pranab/nuam/med/output/_logs
	hadoop fs -rmr /user/pranab/nuam/med/output/_SUCCESS
;;

"medAvDev")
	echo "running mr NumericalAttrMedian for median absolute divergence"
	CLASS_NAME=org.chombo.mr.NumericalAttrMedian
	IN_PATH=/user/pranab/nuam/input
	OUT_PATH=/user/pranab/nuam/mad/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"valCount")
	echo "running mr ValueCounter for counting values"
	CLASS_NAME=org.chombo.mr.ValueCounter
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/vac/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"valCount")
	echo "running mr ValueCounter for counting values"
	CLASS_NAME=org.chombo.mr.ValueCounter
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/vac/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"patCount")
	echo "running mr ValueCounter for counting pattern match"
	CLASS_NAME=org.chombo.mr.PatternMatchedValueCounter
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/pmc/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"uniqCount")
	echo "running mr UniqueCounter for counting unique values"
	CLASS_NAME=org.chombo.mr.UniqueCounter
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/unc/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"uniqKey")
	echo "running mr UniqueKeyAnalyzer for verifying key uniqueness"
	CLASS_NAME=org.chombo.mr.UniqueKeyAnalyzer
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/unk/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"numStat")
	echo "running mr NumericalAttrStats for stats"
	CLASS_NAME=org.chombo.mr.NumericalAttrStats
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/nas/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"numMed")
	echo "running mr NumericalAttrMedian for median etc"
	CLASS_NAME=org.chombo.mr.NumericalAttrMedian
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/nam/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"numDistStat")
	echo "running mr NumericalAttrDistrStats for numerical attr distribution"
	CLASS_NAME=org.chombo.mr.NumericalAttrDistrStats
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/nds/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"catDist")
	echo "running mr CategoricalAttrDistrStats for categorical attr distribution"
	CLASS_NAME=org.chombo.mr.CategoricalAttrDistrStats
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/cas/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"strStat")
	echo "running mr StringAttrStats for string attribute length stats"
	CLASS_NAME=org.chombo.mr.StringAttrStats
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/sas/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"numCorr")
	echo "running mr NumericalCorrelation for numerical attr correlation"
	CLASS_NAME=org.avenir.explore.NumericalCorrelation
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/nuc/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $AVENIR_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"catCorr")
	echo "running mr HeterogeneityReductionCorrelation for categorical attr correlation"
	CLASS_NAME=org.avenir.explore.HeterogeneityReductionCorrelation
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/cac/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $AVENIR_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"funDepend")
	echo "running mr FunctionalDependencyAnalyzer for verifying functional dependency"
	CLASS_NAME=org.chombo.mr.FunctionalDependencyAnalyzer
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/fud/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"mutInfo")
	echo "running mr MutualInformation for feature correlation and relevance"
	CLASS_NAME=org.avenir.explore.MutualInformation
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/mui/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $AVENIR_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"mulDistr")
	echo "running mr MutiVariateDistribution for multi variate distribution"
	CLASS_NAME= org.beymani.dist.MutiVariateDistribution
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/mvd/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $BEYMANI_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"recSim")
	echo "running mr SameTypeSimilarity for record similarity"
	CLASS_NAME=org.sifarish.feature.SameTypeSimilarity
	IN_PATH=/user/pranab/prof/input
	OUT_PATH=/user/pranab/prof/res/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $SIFARISH_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"seasonalityDetector")
	echo "running mr SeasonalDetector for seasonality detection"
	CLASS_NAME=org.chombo.mr.SeasonalDetector
	IN_PATH=/user/pranab/seas/input
	OUT_PATH=/user/pranab/seas/det/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"numAttrStats")
	echo "running mr SeasonalDetector for seasonality detection"
	CLASS_NAME=org.chombo.mr.NumericalAttrStats
	IN_PATH=/user/pranab/seas/input
	OUT_PATH=/user/pranab/seas/sta/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"transformer")
	echo "running mr Transformer for data transformation"
	CLASS_NAME=org.chombo.mr.Transformer
	IN_PATH=/user/pranab/rese/input
	OUT_PATH=/user/pranab/rese/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"checker")
	echo "running mr FormatChecker for median"
	CLASS_NAME=org.chombo.mr.SimpleValidationChecker
	IN_PATH=/user/pranab/foch/input
	OUT_PATH=/user/pranab/foch/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"missingValueCount")
	echo "running mr MissingValueCounter for missing value stats"
	CLASS_NAME=org.chombo.mr.MissingValueCounter
	IN_PATH=/user/pranab/mvco/input
	OUT_PATH=/user/pranab/mvco/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

"missingValueFilter")
	echo "running mr Projection for filtering row and columns for with missing values"
	CLASS_NAME=org.chombo.mr.Projection
	IN_PATH=/user/pranab/mvco/input
	OUT_PATH=/user/pranab/mvfi/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
;;

*) 
	echo "unknown operation $1"
	;;

esac
