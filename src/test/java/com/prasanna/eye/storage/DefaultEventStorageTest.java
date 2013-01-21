package com.prasanna.eye.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kubek2k.springockito.annotations.ReplaceWithMock;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.kubek2k.springockito.annotations.WrapWithSpy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.buffer.EventBuffer;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ContextConfiguration(loader = SpringockitoContextLoader.class,
    locations = {"classpath:/spring/eye-beans.xml", "classpath:/spring/eye-hbase-beans.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class DefaultEventStorageTest extends AbstractJUnit4SpringContextTests {
  @ReplaceWithMock
  @Autowired
  private EventBuffer<String> switchEventBuffer;
  @ReplaceWithMock
  @Autowired
  private EventStorage<String> hbaseEventStorage;

  @WrapWithSpy
  @Autowired
  private TaskScheduler taskScheduler;

  @WrapWithSpy
  @Autowired
  private DefaultEventStorage<String> defaultEventStorage;

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {
    reset(switchEventBuffer, hbaseEventStorage, defaultEventStorage);
  }

  @Test
  public void testSchedulerIsStarted() throws Exception {
    verify(taskScheduler).scheduleAtFixedRate((Runnable) anyObject(), eq(60000L));
    verify(defaultEventStorage).startEventScanning();
  }

  @Test
  public void testSchedulerIsWorking() throws Exception {
    String[] testInput = new String[] {"1", "2", "3"};
    when(switchEventBuffer.flip()).thenReturn(testInput);
    defaultEventStorage.startEventScanning();
    verify(hbaseEventStorage).storeEvents(testInput);
    verify(switchEventBuffer).drain();
  }

  @Test
  public void testStoreEvents() throws Exception {
    String[] testInput = new String[] {"1", "2", "3"};
    defaultEventStorage.storeEvents(testInput);
    verify(switchEventBuffer).storeEvents(testInput);
    verifyNoMoreInteractions(hbaseEventStorage);
  }

  @Test
  public void testQueryEvents() throws Exception {
    QueryModel model = mock(QueryModel.class);
    defaultEventStorage.queryEvents(model);
    verify(hbaseEventStorage).queryEvents(model);
    verifyNoMoreInteractions(switchEventBuffer);
  }
}
