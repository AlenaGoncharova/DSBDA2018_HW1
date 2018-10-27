hadoop jar ./target/HW1_Goncharova-1.0-SNAPSHOT.jar EventsByCity /input /output /dataCityID/city.en.txt
echo "Now results..."
hadoop dfs -text '/output/part-r-*'