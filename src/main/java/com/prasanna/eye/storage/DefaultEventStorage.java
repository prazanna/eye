package com.prasanna.eye.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.List;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.buffer.EventBuffer;

public class DefaultEventStorage<T> implements EventStorage<T> {
  private Logger log = LoggerFactory.getLogger(DefaultEventStorage.class);

  private EventBuffer<T> fastWriteBuffer;
  private EventStorage<T> permanentStorage;

  public void setFastWriteBuffer(final EventBuffer<T> fastWriteBuffer) {
    this.fastWriteBuffer = fastWriteBuffer;
  }

  public void setPermanentStorage(final EventStorage<T> permanentStorage) {
    this.permanentStorage = permanentStorage;
  }

  public void startEventScanning() {
    try {
      log.info("Starting permenant storage scheduler : " + new Date(System.currentTimeMillis()));
      T[] timedEvents = fastWriteBuffer.flip();
      log.info("Found " + timedEvents.length + " events in the buffer");
      permanentStorage.storeEvents(timedEvents);
      fastWriteBuffer.drain();
    } catch(Throwable e) {
      log.error("Storage Scheduler encountered an error", e);
    }
  }

  @Override
  public void storeEvents(final T... timedEvents) {
    // write to the buffer and return back
    fastWriteBuffer.storeEvents(timedEvents);
  }

  @Override
  public List<T> queryEvents(final QueryModel eventQueryModel) {
    // TODO: need to query from both sources
    return permanentStorage.queryEvents(eventQueryModel);
  }
}
