JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
CLASS_NAME=org.chombo.mr.RecordSetBulkMutator

echo "running mr"
IN_PATH=/user/pranab/bumu/master,/user/pranab/bumu/working
OUT_PATH=/user/pranab/bumu/output
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir"

hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/chombo/bumu.properties  $IN_PATH  $OUT_PATH
