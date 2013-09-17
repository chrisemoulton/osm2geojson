package com.github.jillesvangurp.osm2geojson;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.JsonWriter;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.OsmType;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.GeoJsonSupport;
import com.github.jsonj.tools.JsonParser;
import com.jillesvangurp.iterables.LineIterable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author Peter Karich
 */
public class OsmPostProcessorTest {

    @Test
    public void testProcessRelation() throws IOException {
        JsonParser parser = new JsonParser();
        final List<JsonObject> list = new ArrayList<>();
        OsmPostProcessor postProcessor = new OsmPostProcessor(parser) {
            @Override
            protected JsonWriter createJsonWriter(OsmType type) throws IOException {
                return new JsonWriter() {
                    @Override
                    public void add(JsonObject json) throws IOException {
                        list.add(json);
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }
        }.setThreadPoolSize(2).setReadBlockSize(1);
        JsonParser jsonParser = new JsonParser();
        JsonObject o1 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject1.json"))).asObject();
        JsonObject o2 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject2.json"))).asObject();
        LineIterable it = new LineIterable(new StringReader("1;" + o1.toString() + "\n"
                + "2;" + o2.toString() + "\n"));
        postProcessor.processRelations(it, 1);
        assertThat(list.size(), equalTo(2));

        JsonObject o = list.get(0).get("geometry").asObject();
        assertThat(o.getString("type"), is("Polygon"));
        JsonArray arr = o.getArray("coordinates").get(0).asArray();
        assertThat(arr.size(), equalTo(7));

        assertThat(arr.get(2).toString(), equalTo("[2,1]"));
    }
}
