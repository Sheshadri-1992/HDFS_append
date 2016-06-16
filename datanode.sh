#!/bin/bash

if [ $1 == '-c' ]	
then
	javac -sourcepath src -d bin src/**/**/**/*.java 
	echo "Compilation success"

elif [ $1 == '-r' ]
then
	java -cp bin: com.hdfs.datanode.DataNodeDriver $2   #id
else
	java -cp bin: com.hdfs.datanode.DataNodeDriver $1   #id
fi

