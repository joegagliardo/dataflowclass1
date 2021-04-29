#! /bin/sh
rm -r src/main/java/com/mypackage/.ipynb_checkpoints
mvn compile exec:java -Dexec.mainClass="com.mypackage.Simple1"

