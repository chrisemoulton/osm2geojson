package com.github.jillesvangurp.osm2geojson;

import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.set;
import static com.jillesvangurp.iterables.Iterables.compose;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.mergesort.EntryParsingProcessor;
import com.github.jillesvangurp.metrics.LoggingCounter;
import com.github.jillesvangurp.metrics.StopWatch;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.JsonSet;
import com.github.jsonj.tools.JsonParser;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.Processor;
import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Take the osm joined json and convert to a more structured geojson.
 *
 */
public class OsmPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(OsmPostProcessor.class);
    private static final String OSM_POIS_GZ = "osm-pois.gz";
    private static final String OSM_WAYS_GZ = "osm-ways.gz";
    private static final String OSM_RELATIONS_GZ = "osm-relations.gz";
    private final EntryParsingProcessor entryParsingProcessor = new EntryParsingProcessor();
    private final Processor<Entry<String, String>, JsonObject> jsonParsingProcessor;
    private String dir = "./";
    private int threadPoolSize = 4;
    private int readBlockSize = 10;
    private int queueSize = 100;

    public OsmPostProcessor(JsonParser jsonParser) {
        jsonParsingProcessor = new NodeJsonParsingProcessor(jsonParser);
    }

    public OsmPostProcessor setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        return this;
    }

    public OsmPostProcessor setReadBlockSize(int readBlockSize) {
        this.readBlockSize = readBlockSize;
        return this;
    }

    public OsmPostProcessor setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public OsmPostProcessor setDirectory(String dir) {
        this.dir = dir;
        if (!this.dir.endsWith(File.separator))
            this.dir += File.separator;
        return this;
    }

    public interface JsonWriter extends Closeable {

        void add(JsonObject json) throws IOException;
    }

    public enum OsmType {

        POI("poi"), WAY("way"), RELATION("relation");
        private String name;

        private OsmType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    protected JsonWriter createJsonWriter(OsmType type) throws IOException {
        final String location;
        switch (type) {
            case POI:
                location = OSM_POIS_GZ;
                break;
            case WAY:
                location = OSM_WAYS_GZ;
                break;
            case RELATION:
                location = OSM_RELATIONS_GZ;
                break;
            default:
                throw new IllegalArgumentException("cannot happen");
        }

        return new JsonWriter() {
            BufferedWriter out;

            {
                out = ResourceUtil.gzipFileWriter(location);
            }

            @Override
            public void add(JsonObject json) throws IOException {
                out.append(json.toString() + '\n');
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }

    public void processNodes() {
        try {
            processNodes(LineIterable.openGzipFile(dir + OsmJoin.NODE_ID_NODEJSON_MAP), 100000);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void processNodes(LineIterable lineIterable, int logModulo) {
        try {
            try (JsonWriter writer = createJsonWriter(OsmType.POI);
                    LoggingCounter counter = LoggingCounter.counter(LOG, "process nodes", "nodes", logModulo)) {
                Processor<String, JsonObject> p = compose(entryParsingProcessor, jsonParsingProcessor, new Processor<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject process(JsonObject input) {
                        if (input == null)
                            return null;

                        String id = input.getString("id");
                        String name = input.getString("tags", "name");
                        if (name == null)
                            return null;

                        JsonObject geometry = $(_("type", "Point"), _("coordinates", input.getArray("l")));
                        JsonObject geoJson = $(
                                _("id", "osmnode/" + id),
                                _("title", name),
                                _("geometry", geometry));
                        geoJson = interpretTags(input, geoJson);
                        counter.inc();
                        return geoJson;
                    }
                });
                try (ConcurrentProcessingIterable<String, JsonObject> concIt = processConcurrently(lineIterable, p, readBlockSize, threadPoolSize, queueSize)) {
                    for (JsonObject o : concIt) {
                        if (o != null) {
                            writer.add(o);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void processWays() {
        try {
            processWays(LineIterable.openGzipFile(dir + OsmJoin.WAY_ID_COMPLETE_JSON), 100000);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void processWays(LineIterable lineIterable, int logModulo) {
        try {
            try (JsonWriter writer = createJsonWriter(OsmType.WAY);
                    LoggingCounter counter = LoggingCounter.counter(LOG, "process ways", "ways", logModulo)) {
                Processor<String, JsonObject> p = compose(entryParsingProcessor, jsonParsingProcessor, new Processor<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject process(JsonObject input) {
                        String id = input.getString("id");
                        String name = input.getString("tags", "name");
                        if (name == null) {
                            return null;
                        }
                        JsonObject geometry = getWayGeometry(input);
                        JsonObject geoJson = $(
                                _("id", "osmway/" + id),
                                _("title", name),
                                _("geometry", geometry));
                        geoJson = interpretTags(input, geoJson);
                        counter.inc();
                        return geoJson;
                    }
                });
                try (ConcurrentProcessingIterable<String, JsonObject> concIt = processConcurrently(lineIterable, p, readBlockSize, threadPoolSize, queueSize)) {
                    for (JsonObject o : concIt) {
                        if (o != null) {
                            writer.add(o);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonObject getWayGeometry(JsonObject input) {
        JsonArray coordinates = array();
        for (JsonObject n : input.getArray("nodes").objects()) {
            coordinates.add(n.getArray("l"));
        }
        String type = "LineString";
        if (coordinates.get(0).equals(coordinates.get(coordinates.size() - 1))) {
            type = "Polygon";
            JsonArray cs = coordinates;
            coordinates = array();
            coordinates.add(cs);
        }
        JsonObject geometry = $(_("type", type), _("coordinates", coordinates));
        return geometry;
    }

    public void processRelations() {
        try {
            processRelations(LineIterable.openGzipFile(dir + OsmJoin.REL_ID_COMPLETE_JSON), 100000);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void processRelations(LineIterable lineIterable, int logModulo) {
        try {
            try (JsonWriter writer = createJsonWriter(OsmType.RELATION);
                    LoggingCounter counter = LoggingCounter.counter(LOG, "process ways", "ways", logModulo)) {
                Processor<String, JsonObject> p = compose(entryParsingProcessor, jsonParsingProcessor, new Processor<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject process(JsonObject input) {
                        // extract administration bounds
                        // <relation id="2"> <tag k="type" v="boundary" /> or <tag k="boundary" v="administrative" />
                        // fixme: also include boundary=political?
                        // or just type=boundary|multipolygon?
                        String id = input.getString("id");
                        String name = input.getString("tags", "name");
                        if (name == null) {
                            return null;
                        }
                        JsonObject geometry = getGeometryForRelation(input);
                        JsonObject geoJson = $(
                                _("id", "osmrelation/" + id),
                                _("title", name),
                                _("geometry", geometry));
                        geoJson = interpretTags(input, geoJson);
                        counter.inc();

                        // extract admin_levels (60K) multi_polygons
                        // extract public transport routes (62K)
                        // associated street (30K)
                        // TMC ??? some traffic meta data (17K)
                        // restriction on traffic (153K)
                        // rest 34K (mix of all kinds of uncategorized metadata)
                        return geoJson;
                    }
                });
                try (ConcurrentProcessingIterable<String, JsonObject> concIt = processConcurrently(lineIterable, p, readBlockSize, threadPoolSize, queueSize)) {
                    for (JsonObject o : concIt) {
                        if (o != null) {
                            writer.add(o);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonObject getGeometryForRelation(JsonObject input) {
        // TODO ensure orientation of the area (e.g. counter clockwise)
        // TODO use admin_centre for center  

        JsonArray coordinates = array();
        // for administration boundaries we only need ways
        Map<String, JsonObject> ways = new HashMap<>();
        for (JsonObject w : input.getArray("ways").objects()) {
            ways.put(w.getString("id"), w);
        }

        for (JsonObject mem : input.getArray("members").objects()) {
            if ("outer".equals(mem.getString("role"))) {
                JsonObject w = ways.get(mem.getString("id"));
                JsonArray arr = w.getArray("nodes");
                boolean reverse = false;
                if (arr.isEmpty())
                    continue;

                if (!coordinates.isEmpty()) {
                    JsonArray firstPoint = arr.get(0).asObject().getArray("l");
                    JsonArray lastPoint = coordinates.get(coordinates.size() - 1).asArray();
                    if (firstPoint.get(0).asDouble() != lastPoint.get(0).asDouble()
                            || firstPoint.get(1).asDouble() != lastPoint.get(1).asDouble()) {
                        // TODO now it should be 'last of arr' == lastPoint => check that
                        reverse = true;
                    }
                } else {
                    // include the *very* first point
                    coordinates.add(arr.get(0).asObject().getArray("l"));
                }
                // skip the first of every way
                if (reverse) {
                    // reverse is a bit ugly for JsonArray
                    for (int i = arr.size() - 2; i >= 0; i--) {
                        JsonObject n = arr.get(i).asObject();
                        coordinates.add(n.getArray("l"));
                    }
                } else
                    for (int i = 1; i < arr.size(); i++) {
                        JsonObject n = arr.get(i).asObject();
                        coordinates.add(n.getArray("l"));
                    }
            }
        }

        System.out.println(input.get("title") + " -> " + coordinates);
        
        String type = "LineString";
        if (coordinates.get(0).equals(coordinates.get(coordinates.size() - 1))) {
            type = "Polygon";
            JsonArray cs = coordinates;
            coordinates = array();
            coordinates.add(cs);
        }
        JsonObject geometry = $(_("type", type), _("coordinates", coordinates));
        return geometry;
    }

    protected JsonObject interpretTags(JsonObject input, JsonObject geoJson) {
        JsonObject tags = input.getObject("tags");
        JsonObject address = new JsonObject();
        JsonObject name = new JsonObject();
        JsonSet osmCategories = set();
        for (Entry<String, JsonElement> entry : tags.entrySet()) {
            String tagName = entry.getKey();
            String value = entry.getValue().asString();
            if (tagName.startsWith("addr:")) {
                address.put(entry.getKey().substring(5), value);
            } else if (tagName.startsWith("name:")) {
                String language = tagName.substring(5);
                name.getOrCreateArray(language).add(value);
            } else {
                switch (tagName) {
                    case "highway":
                        osmCategories.add("street");
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "leisure":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "amenity":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "natural":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "historic":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "cuisine":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "tourism":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "shop":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "building":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "place":
                        osmCategories.add(tagName + ":" + value);
                        break;
                    case "admin-level":
                    case "boundary":
                        osmCategories.add(tagName + ":" + value);
                        break;
                }
            }
        }

        if (hasPair(tags, "building", "yes")) {
            if (hasPair(tags, "amenity", "public_building")) {
                osmCategories.add("public-building");
            } else {
                osmCategories.add("building");
            }
        }

        if (hasPair(tags, "railway", "tram_stop")) {
            osmCategories.add("tram-stop");
        }

        if (hasPair(tags, "railway", "station")) {
            osmCategories.add("train-station");
        }

        if (hasPair(tags, "railway", "halt")) {
            // may be some rail way crossings included
            osmCategories.add("train-station");
        }

        if (hasPair(tags, "station", "light_rail")) {
            osmCategories.add("light-rail-station");
        }

        if (hasPair(tags, "public_transport", "stop_position")) {
            if (hasPair(tags, "light_rail", "yes")) {
                osmCategories.add("light-rail-station");
            } else if (hasPair(tags, "bus", "yes")) {
                osmCategories.add("bus-stop");
            } else if (hasPair(tags, "railway", "halt")) {
                osmCategories.add("train-station");
            }
        }

        if (osmCategories.size() > 0) {
            geoJson.put("categories", $(_("osm", osmCategories)));
        } else {
            // skip uncategorizable stuff
            return null;
        }
        if (address.size() > 0) {
            geoJson.put("address", address);
        }
        if (tags.containsKey("website")) {
            geoJson.getOrCreateArray("links").add($(_("href", tags.getString("website"))));
        }
        return geoJson;
    }

    protected static boolean hasPair(JsonObject object, String key, String value) {
        String objectValue = object.getString(key);
        if (objectValue != null) {
            return value.equalsIgnoreCase(objectValue);
        } else {
            return false;
        }
    }

    public static void main(String[] args) {
        StopWatch stopWatch = StopWatch.time(LOG, "post process osm");
        OsmPostProcessor processor = new OsmPostProcessor(new JsonParser());
        processor.processNodes();
        processor.processWays();
        processor.processRelations();
        stopWatch.stop();
    }

    private static final class NodeJsonParsingProcessor implements Processor<Entry<String, String>, JsonObject> {

        private final JsonParser parser;

        public NodeJsonParsingProcessor(JsonParser parser) {
            this.parser = parser;
        }

        @Override
        public JsonObject process(Entry<String, String> input) {
            if (input.getValue().length() > 50) {
                return parser.parse(input.getValue()).asObject();
            } else
                return null;
        }
    }
}
