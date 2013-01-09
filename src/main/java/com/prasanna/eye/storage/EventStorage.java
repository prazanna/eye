package com.prasanna.eye.storage;

import com.prasanna.eye.model.TimedEvent;

public interface EventStorage {
  boolean storeEvents(TimedEvent... timedEvents);
}
