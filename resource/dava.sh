JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
CLASS_NAME=org.chombo.mr.ValidationChecker

echo "running mr"
IN_PATH=/usr/avi/chombo/input/dava
OUT_PATH=/usr/avi/chombo/output/dava
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"
hadoop fs -rm /usr/avi/chombo/output/dava/*
echo "removed invalid data file"

hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/ec2-user/pranab/chombo/resource/dava.properties  $IN_PATH  $OUT_PATH
