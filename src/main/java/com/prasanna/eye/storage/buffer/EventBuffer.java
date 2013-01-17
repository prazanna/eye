package com.prasanna.eye.storage.buffer;

import com.prasanna.eye.storage.EventStorage;

public interface EventBuffer<T> extends EventStorage<T> {
  T[] flip();
  void drain();
  Boolean getDrainFlag();
}
