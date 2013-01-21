package com.prasanna.eye.storage.buffer;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
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
import java.io.IOException;
import java.text.ParseException;
import com.prasanna.eye.http.model.TimedEvent;

import static junit.framework.Assert.assertEquals;

@ContextConfiguration(loader = SpringockitoContextLoader.class,
    locations = {"classpath:/spring/eye-beans.xml", "classpath:/spring/eye-hbase-test-beans.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class TestEventSwitchBuffer extends AbstractJUnit4SpringContextTests {
  @Autowired
  private EventSwitchBuffer<TimedEvent> switchEventBuffer;
  private static TimedEvent timedEvent1;
  private static TimedEvent timedEvent2;

  @BeforeClass
  public static void initBeforeClass() throws IOException, ParseException {
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();
    JsonParser jp = factory.createJsonParser(TestTimedEventBuffer.class.getResourceAsStream("/event1.json"));
    JsonNode sampleJsonNode = mapper.readTree(jp);
    timedEvent1 = TimedEvent.fromJson(sampleJsonNode);
    jp = factory.createJsonParser(TestTimedEventBuffer.class.getResourceAsStream("/event2.json"));
    sampleJsonNode = mapper.readTree(jp);
    timedEvent2 = TimedEvent.fromJson(sampleJsonNode);
  }

  @Before
  public void setUp() {
    switchEventBuffer.setPrimaryBuffer(applicationContext.getBean(TimedEventBuffer.class));
    switchEventBuffer.setSecondaryBuffer(applicationContext.getBean(TimedEventBuffer.class));
  }

  @Test
  public void testBufferInsert() {
    switchEventBuffer.storeEvents(timedEvent1);
    switchEventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = switchEventBuffer.flip();
    assertEquals(result.length, 2);
    assertEquals(result[0], timedEvent1);
    assertEquals(result[1], timedEvent2);
  }

  @Test
  public void testBufferFlip() {
    switchEventBuffer.storeEvents(timedEvent1);
    switchEventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = switchEventBuffer.flip();
    assertEquals(result.length, 2);
    switchEventBuffer.storeEvents(timedEvent1);
    switchEventBuffer.storeEvents(timedEvent2);
    result = switchEventBuffer.flip();
    assertEquals(result.length, 2);
  }

  @Test
  /**
   * When I double flip the buffer, it should realize the old buffer is not drained yet
   * and it should return the old set of events
   */
  public void testBufferDoubleFlip() {
    switchEventBuffer.storeEvents(timedEvent1);
    TimedEvent[] result1 = switchEventBuffer.flip();
    assertEquals(result1.length, 1);
    switchEventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result2 = switchEventBuffer.flip();
    assertEquals(result1.length, 1);
    assertEquals(result2[0], timedEvent1);
  }

  @Test
  public void testFlipClearFlip() {
    switchEventBuffer.storeEvents(timedEvent1);
    TimedEvent[] result = switchEventBuffer.flip();
    assertEquals(result.length, 1);
    switchEventBuffer.storeEvents(timedEvent2);
    switchEventBuffer.drain();
    result = switchEventBuffer.flip();
    assertEquals(result.length, 1);
    switchEventBuffer.storeEvents(timedEvent1);
    result = switchEventBuffer.flip();
    assertEquals(result.length, 1);
  }

}
