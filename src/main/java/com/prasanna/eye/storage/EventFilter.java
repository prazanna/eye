package com.prasanna.eye.storage;

import com.prasanna.eye.query.model.QueryModel;

public interface EventFilter<T,K> {
  public void applyRowFilters(final T scan, final QueryModel eventQueryModel);
  public boolean applyDataFilters(final K timedEvent, final QueryModel eventQueryModel);
}
