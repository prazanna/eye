package com.prasanna.eye.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Date;
import com.prasanna.eye.buffer.EventBuffer;
import com.prasanna.eye.model.TimedEvent;

@Component
public class StorageScheduler {
  private Logger log = LoggerFactory.getLogger(StorageScheduler.class);

  @Autowired
  private EventStorage eventStorage;
  @Autowired
  private EventBuffer<TimedEvent> timedEventBuffer;

  @Scheduled(fixedRate = 1000)
  public void persistEvents() {
    log.info("Starting event persister scheduler : " + new Date(System.currentTimeMillis()));
    TimedEvent[] timedEvents = timedEventBuffer.flip();
    boolean result = eventStorage.storeEvents(timedEvents);
    if (result) {
      timedEventBuffer.drain();
    }
    log.info("Result of scheduler : " + new Date(System.currentTimeMillis()));
  }
}
