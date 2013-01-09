package com.prasanna.eye.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Override
  public void offer(final T event) {
    this.primaryBuffer.offer(event);
  }

  @Override
  public T[] flip() {
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

  @Override
  public void drain() {
    secondaryBuffer.drain();
  }

  @Override
  public Boolean getDrainFlag() {
    return primaryBuffer.getDrainFlag();
  }
}
