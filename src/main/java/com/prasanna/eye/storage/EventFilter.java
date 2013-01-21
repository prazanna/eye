package com.prasanna.eye.storage;

import org.apache.hadoop.hbase.client.Scan;
import java.util.List;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.query.model.QueryPredicateModel;

public interface EventFilter<T,K> {
  public void applyRowFilters(final Scan scan, final List<QueryPredicateModel> eventQueryModel);
  public boolean applyDataFilters(final TimedEvent timedEvent, final List<QueryPredicateModel> eventQueryModel);
}
