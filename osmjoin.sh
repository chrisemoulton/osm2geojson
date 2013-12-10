# run mvn clean install first
# big heap needed to allow for large bucket size when sorting (less filehandles)

APP_HOME=$(dirname $0)
JAVA=$JAVA_HOME/bin/java
if [ "x$JAVA_HOME" = "x" ]; then
 JAVA=java
fi

if [ "x$JAVA_OPTS" = "x" ]; then
 JAVA_OPTS="-server -Xmx1000m"
fi

$JAVA -cp $APP_HOME/target/osm2geojson-1.0-SNAPSHOT.jar:$APP_HOME/target/lib/* $JAVA_OPTS com.github.jillesvangurp.osm2geojson.OsmJoin "$1" | tee osmjoin.log