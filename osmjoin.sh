# run mvn clean install first
# big heap needed to allow for large bucket size when sorting (less filehandles)
JAVA=$JAVA_HOME/bin/java
if [ "x$JAVA_HOME" = "x" ]; then
 JAVA=java
fi

if [ "x$JAVA_OPTS" = "x" ]; then
 JAVA_OPTS="-server -Xmx1000m"
fi

$JAVA -cp target/osm2geojson-1.0-SNAPSHOT.jar:target/lib/* $JAVA_OPTS com.github.jillesvangurp.osm2geojson.OsmJoin "$1" | tee osmjoin.log