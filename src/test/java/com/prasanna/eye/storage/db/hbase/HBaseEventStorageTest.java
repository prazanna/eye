package com.prasanna.eye.storage.db.hbase;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.List;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.EventQueryParser;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.buffer.TestTimedEventBuffer;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = {
    "classpath:/spring/eye-hbase-test-beans.xml",
    "classpath:/spring/eye-beans.xml"
    })
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class HBaseEventStorageTest extends AbstractJUnit4SpringContextTests {
  private static TimedEvent timedEvent1;
  private static TimedEvent timedEvent2;
  private static QueryModel queryModel;
  @Autowired
  private HBaseEventStorage hBaseEventStorage;

  @BeforeClass
  public static void initBeforeClass() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();
    JsonParser jp = factory.createJsonParser(TestTimedEventBuffer.class.getResourceAsStream("/event1.json"));
    JsonNode sampleJsonNode = mapper.readTree(jp);
    timedEvent1 = TimedEvent.fromJson(sampleJsonNode);
    jp = factory.createJsonParser(TestTimedEventBuffer.class.getResourceAsStream("/event2.json"));
    sampleJsonNode = mapper.readTree(jp);
    timedEvent2 = TimedEvent.fromJson(sampleJsonNode);

    EventQueryParser queryParser = new EventQueryParser();
    queryModel = queryParser.parseEventQuery("request1.within(\"1990/07/04 12:08:56 PST\", \"2012/07/04 12:08:56 PST\")");
//    queryModel = queryParser.parseEventQuery("request.last(\"4hours\").eq(/\"service\"," +
//        "#\"Cluster\").eq(/\"service\",#\"Cluster\")");

  }

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testStoreEvents() throws Exception {
    hBaseEventStorage.storeEvents(timedEvent1, timedEvent2);
    List<TimedEvent> results = hBaseEventStorage.queryEvents(queryModel);
    System.out.println(results);
  }

  @Test
  public void testQueryEvents() throws Exception {

  }
}
