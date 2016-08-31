#!/bin/sh

CLASSPATH_HADOOP="$HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.4.jar:\
$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.4.jar:\
$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar"

ee()
{
	echo ">" $@
	eval $@
}

cd $(dirname $0)

# 0
ee "hdfs dfs -rm -r input"
ee "hdfs dfs -rm -r output"

# 1
ee "$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode"
ee "$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode"
ee "hdfs dfs -mkdir -p input"
ee "hdfs dfs -put data/users.xml input/users.xml"
ee "hdfs dfs -ls input"

# 2
mkdir -p out/classes
ee "javac -classpath $CLASSPATH_HADOOP -d out/classes src/se/kth/id2221/TopTen.java"
ee "jar cvf out/topten.jar -C out/classes ."

# 3
ee "hadoop jar out/topten.jar se.kth.id2221.TopTen input output"

# 4
ee "hdfs dfs -ls output"
ee "hdfs dfs -cat output/part-r-00000"
