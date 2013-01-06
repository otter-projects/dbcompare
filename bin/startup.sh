#!/bin/bash
CLASSPATH="../conf/"
for i in ../lib/*;
        do CLASSPATH=$i:"$CLASSPATH";
done

JAVA_OPTS="-server -Xms512m -Xmx1024m -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -Xss256k -XX:-UseAdaptiveSizePolicy -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError"
if [ "$1" = "debug" ]; then
	DEBUG_PORT=$2
	DEBUG_SUSPEND="n"
	JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$DEBUG_PORT,server=y,suspend=$DEBUG_SUSPEND"
fi
		
nohup java $JAVA_OPTS $JAVA_DEBUG_OPT -Dlogback.configurationFile=../conf/logback.xml  -classpath .:$CLASSPATH com.zavakid.dbcompare.CompareLauncher  &
echo $! > compare.pid 
