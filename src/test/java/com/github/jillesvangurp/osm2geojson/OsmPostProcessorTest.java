package com.github.jillesvangurp.osm2geojson;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.JsonWriter;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.OsmType;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
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
        JsonObject o0 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject1.json"))).asObject();
        JsonObject o1 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject2.json"))).asObject();
        JsonObject o2 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject3.json"))).asObject();
        // Landkreis Hof is problematic:
        // see "Way Germany - Czech Republic (166216396)" and "Way 148994491" where "Way 235603311" is missing and just appended! Probably due to the multipolygon stuff
        // http://www.openstreetmap.org/browse/relation/2145179
        JsonObject o3 = jsonParser.parse(ResourceUtil.string(getClass().getResourceAsStream("relobject4.json"))).asObject();
        LineIterable it = new LineIterable(new StringReader("0;" + o0.toString() + "\n"
                + "1;" + o1.toString() + "\n"
                + "2;" + o2.toString() + "\n"
                + "3;" + o3.toString() + "\n"));
        postProcessor.processRelations(it, 1);
        assertThat(list.size(), equalTo(4));

        JsonObject o = list.get(0).get("geometry").asObject();
        assertThat(o.getString("type"), is("Polygon"));
        JsonArray arr = o.getArray("coordinates").get(0).asArray();
        // 1,2,12,13,14,45,1
        assertThat(arr.toString(), equalTo("[[0,1],[1,0],[2,1],[1.6,1.5],[1,1.6],[0.4,1.5],[0,1]]"));

        // reverse order of what is in members
        // 2nd way. way id 146126567
        assertThat(list.get(2).getObject("geometry").getArray("coordinates").get(0).asArray().get(5).toString(),
                equalTo("[10.9979997,48.2868964]"));
        // last way. first and last of way id 146086042
        assertThat(list.get(2).getObject("geometry").getArray("coordinates").get(0).asArray().get(0).toString(),
                equalTo("[10.9505042,48.3266102]"));
        assertThat(list.get(2).getObject("geometry").getArray("coordinates").get(0).asArray().get(9).toString(),
                equalTo("[11.023158,48.297985]"));

        assertThat(list.get(3).toString(), equalTo(list.get(2).toString()));
    }
}
