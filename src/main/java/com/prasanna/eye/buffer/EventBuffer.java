package com.prasanna.eye.buffer;

public interface EventBuffer<T> {
  void offer(T timedEvent);
  T[] flip();
  void drain();
  Boolean getDrainFlag();
}
