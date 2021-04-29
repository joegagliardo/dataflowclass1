#! /bin/sh
rm -r src/main/java/com/mypackage/.ipynb_checkpoints 2> /dev/null
rm regions_out* 2> /dev/null
mvn compile exec:java -Dexec.mainClass="com.mypackage.Simple1"

