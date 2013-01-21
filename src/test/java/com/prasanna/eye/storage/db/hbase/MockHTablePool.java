package com.prasanna.eye.storage.db.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class MockHTablePool extends HTablePool{

  @Override
  protected HTableInterface createHTable(String tableName) {
    return MockHTable.create(tableName);
  }
}
