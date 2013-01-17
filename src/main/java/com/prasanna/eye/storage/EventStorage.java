package com.prasanna.eye.storage;

import java.util.List;
import com.prasanna.eye.query.model.QueryModel;

public interface EventStorage<T> {
  void storeEvents(T... timedEvents);
  List<T> queryEvents(QueryModel eventQueryModel);
}
