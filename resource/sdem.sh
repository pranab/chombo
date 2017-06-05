# contains everything needed to execute chombo in batch mode

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
HDFS_BASE_DIR=/user/pranab/sdem
PROP_FILE=/home/pranab/Projects/bin/chombo/sdem.properties
HDFS_META_BASE_DIR=/user/pranab/sdem/meta

case "$1" in

"genInp")
	./seasonal_xaction.py $2 $3 > $4
	 ls -l $4
;;

"loadInp")
	hadoop fs -rm $HDFS_BASE_DIR/sed/input/$2
	hadoop fs -put $2 $HDFS_BASE_DIR/sed/input
	hadoop fs -ls $HDFS_BASE_DIR/sed/input
;;

"seasonalityDetector")
	echo "running mr SeasonalDetector for seasonality detection"
	CLASS_NAME=org.chombo.mr.SeasonalDetector
	IN_PATH=$HDFS_BASE_DIR/sed/input
	OUT_PATH=$HDFS_BASE_DIR/sed/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/sed/output/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/sed/output/_SUCCESS
	echo "showing output"
	hadoop fs -ls $HDFS_BASE_DIR/sed/output
;;

"numDistStat")
	echo "running mr NumericalAttrDistrStats for numerical attr distribution"
	CLASS_NAME=org.chombo.mr.NumericalAttrDistrStats
	IN_PATH=$HDFS_BASE_DIR/sed/output
	OUT_PATH=$HDFS_BASE_DIR/nds/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/nds/output/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/nds/output/_SUCCESS
	echo "showing output"
	hadoop fs -ls $HDFS_BASE_DIR/nds/output
;;

"seasonalCycleFinder")
	echo "running mr SeasonalCycleFinder for finding cycles"
	CLASS_NAME=org.chombo.mr.SeasonalCycleFinder
	IN_PATH=$HDFS_BASE_DIR/nds/output
	OUT_PATH=$HDFS_BASE_DIR/scf/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/scf/output/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/scf/output/_SUCCESS
	echo "showing output"
	hadoop fs -ls $HDFS_BASE_DIR/scf/output
;;

*) 
	echo "unknown operation $1"
	;;

esac
