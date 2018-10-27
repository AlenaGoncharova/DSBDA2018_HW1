#download script
hdfs dfs -mkdir /input
hadoop fs -rm /input/*
hdfs dfs -mkdir /dataCityID
hadoop fs -rm /dataCityID/*
hadoop fs -rm -r /output
hdfs dfs -put downloadsHW1/city.en.txt /dataCityID
rm downloadsHW1/city.en.txt
hdfs dfs -put downloadsHW1/* /input
hdfs dfs -ls /input