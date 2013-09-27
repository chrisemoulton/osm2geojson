package com.github.jillesvangurp.osm2geojson;

import com.github.jillesvangurp.common.ResourceUtil;
import com.github.jillesvangurp.mergesort.SortingWriter;
import com.github.jsonj.tools.JsonParser;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import static org.testng.Assert.assertTrue;

import java.util.regex.Matcher;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class OsmJoinTest {

    @DataProvider
    public Object[][] sampleNodes() {
        return new Object[][] {
                {"<node id=\"25737250\" lat=\"51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", 51.5121071,-0.1130375},
                {"<node id=\"25737250\" lat=\"-51.5121071\" lon=\"-0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071,-0.1130375},
                {"<node id=\"25737250\" lat=\"-51.5121071\" lon=\"0.1130375\" timestamp=\"2010-12-10T23:35:50Z\" version=\"3\" changeset=\"6613493\" user=\"Welshie\" uid=\"508\"/>", -51.5121071,0.1130375}
        };
    }

    @Test(dataProvider="sampleNodes")
    public void shouldFindCoordinate(String nodexml, double latitude, double longitude) {
        Matcher latMatcher = OsmJoin.latPattern.matcher(nodexml);
        assertTrue(latMatcher.find());
        assertThat(latMatcher.group(1), is(""+latitude));
        Matcher lonMatcher = OsmJoin.lonPattern.matcher(nodexml);
        assertTrue(lonMatcher.find());
        assertThat(lonMatcher.group(1), is(""+longitude));
    }
    
    @Test 
    public void shouldParseRelation() throws IOException {
        JsonParser parser = new JsonParser();
        OsmJoin join = new OsmJoin(null, parser);
        final Map<String, String> relMap = new HashMap<>();
        final Map<String, String> nodeId2wayMap = new HashMap<>();
        final Map<String, String> wayId2relMap = new HashMap<>();
                
        SortingWriter relationsWriter = new SortingWriter(null, null, 0) {
            @Override public void put(String key, String value) {
                relMap.put(key, value);
            }
        };
        SortingWriter nodeId2wayWriter = new SortingWriter(null, null, 0) {
            @Override public void put(String key, String value) {
                nodeId2wayMap.put(key, value);
            }
        };
        SortingWriter wayId2RelWriter = new SortingWriter(null, null, 0) {
            @Override public void put(String key, String value) {
                wayId2relMap.put(key, value);
            }
        };
        String input = ResourceUtil.string(getClass().getResourceAsStream("relation1.xml"));
        join.parseRelation(relationsWriter, new BufferedWriter(new StringWriter()),
                nodeId2wayWriter, wayId2RelWriter, input);     
        assertThat(relMap.size(), equalTo(1));
        assertThat(wayId2relMap.size(), equalTo(4));
    }
}
