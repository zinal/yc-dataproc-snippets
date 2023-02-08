#! /bin/sh

JAR=`ls lib/sample-producer-*.jar`
java -jar ${JAR} $@
