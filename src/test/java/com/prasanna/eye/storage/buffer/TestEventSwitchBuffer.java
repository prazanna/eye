package com.prasanna.eye.storage.buffer;

import org.apache.commons.collections.BufferOverflowException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.io.IOException;
import java.text.ParseException;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.storage.buffer.EventSwitchBuffer;
import com.prasanna.eye.storage.buffer.TimedEventBuffer;

import static junit.framework.Assert.assertEquals;

@ContextConfiguration("classpath:/spring/eye-beans.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class TestEventSwitchBuffer extends AbstractJUnit4SpringContextTests {
  @Autowired
  private EventSwitchBuffer<TimedEvent> eventBuffer;
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
    eventBuffer.setPrimaryBuffer(applicationContext.getBean(TimedEventBuffer.class));
    eventBuffer.setSecondaryBuffer(applicationContext.getBean(TimedEventBuffer.class));
  }

  @Test
  public void testBufferInsert() {
    eventBuffer.storeEvents(timedEvent1);
    eventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = eventBuffer.flip();
    assertEquals(result.length, 2);
    assertEquals(result[0], timedEvent1);
    assertEquals(result[1], timedEvent2);
  }

  @Test
  public void testBufferFlip() {
    eventBuffer.storeEvents(timedEvent1);
    eventBuffer.storeEvents(timedEvent2);
    TimedEvent[] result = eventBuffer.flip();
    assertEquals(result.length, 2);
    eventBuffer.storeEvents(timedEvent1);
    eventBuffer.storeEvents(timedEvent2);
    result = eventBuffer.flip();
    assertEquals(result.length, 2);
  }

  @Test(expected = BufferOverflowException.class)
  public void testBufferDoubleFlip() {
    eventBuffer.storeEvents(timedEvent1);
    TimedEvent[] result = eventBuffer.flip();
    assertEquals(result.length, 1);
    eventBuffer.storeEvents(timedEvent2);
    result = eventBuffer.flip();
    assertEquals(result.length, 1);
    eventBuffer.storeEvents(timedEvent1);
  }

  @Test
  public void testFlipClearFlip() {
    eventBuffer.storeEvents(timedEvent1);
    TimedEvent[] result = eventBuffer.flip();
    assertEquals(result.length, 1);
    eventBuffer.storeEvents(timedEvent2);
    eventBuffer.drain();
    result = eventBuffer.flip();
    assertEquals(result.length, 1);
    eventBuffer.storeEvents(timedEvent1);
    result = eventBuffer.flip();
    assertEquals(result.length, 1);
  }

}
