#!/bin/bash
if [ $1 == '-c' ]	
then
	javac -sourcepath src -d bin src/**/**/**/*.java 
	echo "Compilation success"

elif [ $1 == '-r' ]
then
	java -cp bin: com.hdfs.client.ClientDriver $2 $3   #filename operation
else
	java -cp bin: com.hdfs.client.ClientDriver $1 $2   #filename operation
fi

