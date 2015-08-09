JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
CLASS_NAME=org.chombo.mr.ValidationChecker

echo "running mr"
IN_PATH=/user/pranab/dava/input
OUT_PATH=/user/pranab/dava/output
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"
hadoop fs -rm /user/pranab/output/dava/*
echo "removed invalid data file"

hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/chombo/dava.properties  $IN_PATH  $OUT_PATH
