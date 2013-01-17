package com.prasanna.eye.storage.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import java.util.List;
import com.google.common.collect.Lists;
import com.prasanna.eye.query.model.QueryModel;

public class EventSwitchBuffer<T> implements EventBuffer<T>{
  private EventBuffer<T> primaryBuffer;
  private EventBuffer<T> secondaryBuffer;
  private Logger log = LoggerFactory.getLogger(EventSwitchBuffer.class);

  public void setPrimaryBuffer(final EventBuffer<T> primaryBuffer) {
    this.primaryBuffer = primaryBuffer;
  }

  public void setSecondaryBuffer(final EventBuffer<T> secondaryBuffer) {
    this.secondaryBuffer = secondaryBuffer;
  }

  public synchronized void storeEvents(final T... event) {
    this.primaryBuffer.storeEvents(event);
  }

  @Override
  public List<T> queryEvents(final QueryModel eventQueryModel) {
    return Lists.newArrayList();
  }

  public synchronized T[] flip() {
    if(secondaryBuffer.getDrainFlag()) {
      log.info("Secondary buffer is not drained yet, Returning secondary data");
      return secondaryBuffer.flip();
    }
    T[] events = primaryBuffer.flip();
    EventBuffer<T> temp = primaryBuffer;
    primaryBuffer = secondaryBuffer;
    secondaryBuffer = temp;
    log.info("Switching buffers after flip call");
    return events;
  }

  public synchronized void drain() {
    secondaryBuffer.drain();
  }

  public Boolean getDrainFlag() {
    return primaryBuffer.getDrainFlag();
  }
}
