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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
                        JsonObject geoJson = $(
                                _("id", "osmway/" + id),
                                _("title", name));
                        handleWay(input, geoJson);

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

    protected void handleWay(JsonObject input, JsonObject output) {
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

        output.put("geometry", $(_("type", type), _("coordinates", coordinates)));
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
                    LoggingCounter counter = LoggingCounter.counter(LOG, "process relations", "relations", logModulo)) {
                Processor<String, JsonObject> p = compose(entryParsingProcessor, jsonParsingProcessor, new Processor<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject process(JsonObject input) {
                        String id = input.getString("id");
                        String name = input.getString("tags", "name");
                        if (name == null)
                            return null;

                        // extract only administration bounds for now
                        JsonObject geoJson = $(
                                _("id", "osmrelation/" + id),
                                _("title", name));
                        handleRelation(input, geoJson);
                        if (!geoJson.containsKey("geometry"))
                            return null;

                        geoJson = interpretTags(input, geoJson);
                        counter.inc();

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

    /**
     * WayManager is used to make arbitrary ordering of the ways possible and
     * they'll be connected via their id. See e.g. 'Landkreis Hof'
     * http://www.openstreetmap.org/browse/relation/2145179 which has a multi
     * polygon and 'Way 235603311' is not at the correct place (between "Way
     * Germany - Czech Republic (166216396)" and "Way 148994491")
     */
    class WayManager {

        private final Object relId;
        private final String name;
        private Map<Long, NodeIdEntry> nodeIds2Ways = new LinkedHashMap<>();

        public WayManager(Object relId, String name) {
            this.relId = relId;
            this.name = name;
        }

        public boolean isEmpty() {
            return nodeIds2Ways.isEmpty();
        }

        public NodeIdEntry getFirst() {
            return nodeIds2Ways.values().iterator().next();
        }

        public NodeIdEntry get(long id) {
            NodeIdEntry e = nodeIds2Ways.get(id);
            return e;
        }

        public boolean remove(long id) {
            return nodeIds2Ways.remove(id) != null;
        }

        public void add(JsonArray arr) {
            long firstId = arr.get(0).asObject().getLong("id");
            long lastId = arr.get(arr.size() - 1).asObject().getLong("id");

            add(firstId, lastId, arr);
            // do not use Collection.reverse here as it also changes old arr
            add(lastId, firstId, reverse(arr));
        }

        private void add(long nodeId, long nextNodeId, JsonArray arr) {
            NodeIdEntry e = nodeIds2Ways.get(nodeId);
            if (e == null) {
                e = new NodeIdEntry();
                e.first = arr;
                e.id = nodeId;
                e.firstNextId = nextNodeId;
                nodeIds2Ways.put(nodeId, e);
            } else {
                if (e.first == null)
                    throw new IllegalStateException("first pointer should be already assigned. node:" + nodeId + ", relId:" + relId + "," + name);
                if (e.second != null) {
                    // ignore loops like in 'Weißenburg-Gunzenhausen' http://www.openstreetmap.org/browse/relation/62390 -> see way 194213149
                    LOG.warn("second pointer was already assigned. node:" + nodeId + ", relId:" + relId + "," + name);
                    return;
                }
                e.secondNextId = nextNodeId;
                e.second = arr;
            }
        }
    }

    /**
     * Every id belongs to two arrays. Helpful to traverse and connect/build one
     * polygon
     */
    class NodeIdEntry {

        long id;
        long firstNextId = -1, secondNextId = -1;
        JsonArray first;
        JsonArray second;

        @Override public String toString() {
            return id + " " + firstNextId + " " + secondNextId;
        }
    }

    /**
     * Creates (multi) polygon out of the provides ways. Algorithm is robust if
     * out of order. But there is currently no guarantee which ensures the
     * orientation of the area (e.g. counter clockwise). Only outer ways for now
     * are recognized.
     */
    protected void handleRelation(JsonObject input, JsonObject output) {
        Map<String, JsonObject> ways = new HashMap<>();
        Object id = input.getObject("tags").get("id");
        String name = input.getObject("tags").get("name").asString();
        for (JsonObject w : input.getArray("ways").objects()) {
            ways.put(w.getString("id"), w);
        }

        WayManager wayManager = new WayManager(id, name);
        for (JsonObject mem : input.getArray("members").objects()) {
            String role = mem.getString("role");
            if ("outer".equals(role)) {
                JsonObject w = ways.get(mem.getString("id"));
                wayManager.add(w.getArray("nodes"));

            } else if ("admin_centre".equals(role) && "node".equals(mem.getString("type"))) {
                if (output.containsKey("admin_centre"))
                    LOG.warn("multiple admin_centre exist!? " + output.get("admin_centre") + ", " + id + "," + name);
                else
                    output.put("admin_centre", mem.getString("id"));
            }
        }

        JsonArray coordinates = array();
        while (!wayManager.isEmpty()) {
            NodeIdEntry first = wayManager.getFirst(), e = first;
            long nextId = e.firstNextId;
            JsonArray arr = e.first;
            JsonArray outerBoundary = array();
            if (!arr.isEmpty())
                outerBoundary.add(arr.get(0).asObject().getArray("l"));

            // now loop through the ways until one polygon is formed
            while (true) {
                // skip the first coordinate of every way to avoid duplicates
                for (int i = 1; i < arr.size(); i++) {
                    JsonObject n = arr.get(i).asObject();
                    outerBoundary.add(n.getArray("l"));
                }
                long oldId = e.id;
                if (!wayManager.remove(e.id))
                    throw new IllegalStateException("Cannot remove id " + e.id + ". Something went wrong " + input);

                e = wayManager.get(nextId);
                if (e == null)
                    break;

                if (oldId == e.firstNextId) {
                    nextId = e.secondNextId;
                    arr = e.second;
                } else {
                    nextId = e.firstNextId;
                    arr = e.first;
                }
                if (arr == null)
                    throw new IllegalStateException("Array must not be null. Something is wrong with: " + input);
            }

            if (outerBoundary.isEmpty())
                continue;

            if (!outerBoundary.get(0).equals(outerBoundary.get(outerBoundary.size() - 1)))
                continue;

            JsonArray polygon = array();
            polygon.add(outerBoundary);
            // no holes supported yet
            // polygon.add(holes);
            coordinates.add(polygon);
        }

        if (coordinates.isEmpty())
            return;

        JsonObject geometry;
        if (coordinates.size() == 1) {
            // A polygon is defined by a list of a list of points. The first and last points in each
            // list must be the same (the polygon must be closed). The first array represents the outer 
            // boundary of the polygon (unsupported: the other arrays represent the interior shapes (holes))
            geometry = $(_("type", "Polygon"), _("coordinates", coordinates.get(0)));
        } else {
            // multiple polygons
            geometry = $(_("type", "MultiPolygon"), _("coordinates", coordinates));
        }
        output.put("geometry", geometry);
    }

    public static JsonArray reverse(JsonArray arr) {
        JsonArray res = new JsonArray();
        for (int i = arr.size() - 1; i >= 0; i--) {
            JsonElement el = arr.get(i);
            res.add(el);
        }
        return res;
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
