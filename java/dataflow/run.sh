#! /bin/sh
rm -r src/main/java/com/mypackage/.ipynb_checkpoints 2> /dev/null
# rm regions_out* 2> /dev/null
rm -r output 2> /dev/null
mkdir output 2> /dev/null
mvn compile exec:java -Dexec.mainClass="com.mypackage.$1"
cat output/*

