#!/bin/sh

for f in libs/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done

for f in target/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done

CLASS=com.wolfit.selector_threads.App

~/java/bin/java -classpath ${CLASSPATH} ${CLASS}
