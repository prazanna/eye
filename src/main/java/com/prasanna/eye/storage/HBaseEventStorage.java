package com.prasanna.eye.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import com.google.common.collect.Lists;
import com.prasanna.eye.buffer.ObjectSerializer;
import com.prasanna.eye.model.TimedEvent;

@Component
public class HBaseEventStorage implements EventStorage {
  private HTablePool hTablePool;
  private byte[] EVENTS_CF = Bytes.toBytes("events");

  @PostConstruct
  public void init() {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", "m0106.mtv.cloudera.com");
    config.set("hbase.zookeeper.property.clientPort", "2181");

    hTablePool = new HTablePool(config, 3);
    hTablePool.putTable(hTablePool.getTable("eye_events"));
  }

  @Override
  public boolean storeEvents(final TimedEvent... timedEvents) {
    List<Put> puts = Lists.newArrayList();
    for(TimedEvent te:timedEvents) {
      Put put = new Put(Bytes.toBytes(Long.MAX_VALUE - te.getTime().getTime()));
      put.add(EVENTS_CF, Bytes.toBytes(te.getType()), ObjectSerializer.serialize(te));
    }
    try {
      hTablePool.getTable("eye_events").put(puts);
    } catch (IOException e) {
      try {
        hTablePool.getTable("eye_events").flushCommits();
      } catch (IOException e1) {
        // ignore
      }
      return false;
    }
    return true;
  }
}
