#download script
hdfs dfs -mkdir /input
hdfs dfs -rm /input/*
hdfs dfs -mkdir /dataCityID
hdfs fs -rm /dataCityID/*
hdfs fs -rm -r /output
hdfs dfs -put downloadsHW1/city.en.txt /dataCityID
rm downloadsHW1/city.en.txt
hdfs dfs -put downloadsHW1/* /input
hdfs dfs -ls /input