package com.prasanna.eye.storage.db.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import com.google.common.collect.Lists;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.EventStorage;
import com.prasanna.eye.storage.buffer.ObjectSerializer;
import com.prasanna.eye.storage.db.StorageException;
import com.prasanna.eye.storage.db.hbase.filters.HBaseEventDefaultFilter;

public class HBaseEventStorage implements EventStorage<TimedEvent> {
  private Logger log = LoggerFactory.getLogger(HBaseEventStorage.class);
  private HTablePool hTablePool;
  private HBaseEventDefaultFilter filter;
  private byte[] EVENTS_CF = Bytes.toBytes("events");

  public void setFilter(final HBaseEventDefaultFilter filter) {
    this.filter = filter;
  }

  public void init() {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", "m0106.mtv.cloudera.com");
    config.set("hbase.zookeeper.property.clientPort", "2181");

    hTablePool = new HTablePool(config, 3);
    hTablePool.putTable(hTablePool.getTable("eye_events"));
  }

  public void storeEvents(final TimedEvent... timedEvents) {
    List<Put> puts = Lists.newArrayList();
    for (TimedEvent te : timedEvents) {
      Put put = new Put(Bytes.toBytes(Long.MAX_VALUE - te.getTime().getTime()));
      put.add(EVENTS_CF, Bytes.toBytes(te.getType()), ObjectSerializer.serialize(te));
      puts.add(put);
    }
    try {
      hTablePool.getTable("eye_events").put(puts);
      log.info("Stored " + puts.size()+" events into hbase");
    } catch (IOException e) {
      try {
        hTablePool.getTable("eye_events").flushCommits();
      } catch (IOException ex) {
        // ignore. we will be throwing a storage exception
      }
      throw new StorageException();
    }
  }

  @Override
  public List<TimedEvent> queryEvents(final QueryModel eventQueryModel) {
    HTableInterface table = hTablePool.getTable("eye_events");
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(0L));
    scan.setStopRow(Bytes.toBytes(Long.MAX_VALUE));
    filter.applyRowFilters(scan, eventQueryModel);
    ResultScanner results;
    try {
      results = table.getScanner(scan);
    } catch (IOException e) {
      throw new StorageException();
    }

    Iterator<Result> iterator = results.iterator();
    List<TimedEvent> timedEvents = Lists.newArrayList();
    while (iterator.hasNext()) {
      Result result = iterator.next();
      KeyValue dbResult = result.getColumnLatest(EVENTS_CF, Bytes.toBytes(eventQueryModel.getEventType()));
      TimedEvent timedEvent = ObjectSerializer.deSerialize(ByteBuffer.wrap(dbResult.getValue()), TimedEvent.class);
      boolean filterResult = filter.applyDataFilters(timedEvent, eventQueryModel);
      if(filterResult)
        timedEvents.add(timedEvent);
    }
    return timedEvents;
  }
}
