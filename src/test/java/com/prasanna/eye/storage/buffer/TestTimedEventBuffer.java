package com.prasanna.eye.storage.buffer;

import org.apache.commons.collections.BufferOverflowException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kubek2k.springockito.annotations.ReplaceWithMock;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.io.IOException;
import java.text.ParseException;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.storage.EventStorage;

import static junit.framework.Assert.assertEquals;

@ContextConfiguration(loader = SpringockitoContextLoader.class,
    locations = {"classpath:/spring/eye-beans.xml", "classpath:/spring/eye-hbase-test-beans.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class TestTimedEventBuffer {
  @Autowired
  private TimedEventBuffer defaultEventBuffer;
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

  @Test
  public void testBufferInsert() {
    defaultEventBuffer.storeEvents(timedEvent1);
    defaultEventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = defaultEventBuffer.flip();
    assertEquals(result.length, 2);
    assertEquals(result[0], timedEvent1);
    assertEquals(result[1], timedEvent2);
  }

  @Test(expected = BufferOverflowException.class)
  public void testBufferFlip() {
    defaultEventBuffer.storeEvents(timedEvent1);
    TimedEvent[] result = defaultEventBuffer.flip();
    assertEquals(result.length, 1);
    defaultEventBuffer.storeEvents(timedEvent1);
  }

  @Test
  public void testBufferClear() {
    defaultEventBuffer.storeEvents(timedEvent1);
    defaultEventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = defaultEventBuffer.flip();
    assertEquals(result.length, 2);
    defaultEventBuffer.drain();
    assertEquals(defaultEventBuffer.bufferFile.length(), 0);
    defaultEventBuffer.storeEvents(timedEvent1);
    defaultEventBuffer.storeEvents(timedEvent2);
    result = defaultEventBuffer.flip();
    assertEquals(result.length, 2);
  }

}
