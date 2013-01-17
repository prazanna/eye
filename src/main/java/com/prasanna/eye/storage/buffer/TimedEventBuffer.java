package com.prasanna.eye.storage.buffer;

import org.apache.commons.collections.BufferOverflowException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import com.google.common.collect.Lists;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.model.QueryModel;

public class TimedEventBuffer implements EventBuffer<TimedEvent> {
  private volatile Boolean drainFlag = Boolean.FALSE;
  private FileChannel fileChannel;
  private Logger log = LoggerFactory.getLogger(TimedEventBuffer.class);
  File bufferFile;

  private void init() {
    try {
      bufferFile = File.createTempFile("eye", "event.buffer");
      bufferFile.deleteOnExit();
      RandomAccessFile raf = new RandomAccessFile(bufferFile, "rw");
      fileChannel = raf.getChannel();
      log.info("Initialized Event buffer on RAF " + bufferFile.getAbsolutePath());
    } catch (IOException e) {
      throw new BufferException(e);
    }
  }

  private void cleanup() {
    if(bufferFile.exists()) {
      //noinspection ResultOfMethodCallIgnored
      bufferFile.delete();
    }
  }

  public Boolean getDrainFlag() {
    return drainFlag;
  }

  public void storeEvents(final TimedEvent... timedEvents) {
    if (drainFlag) {
      throw new BufferOverflowException();
    }

    synchronized (this) {
      if (drainFlag) {
        throw new BufferOverflowException();
      }
      for(TimedEvent timedEvent:timedEvents) {
        try {
          byte[] eventData =  ObjectSerializer.serialize(timedEvent);
          fileChannel.write(ByteBuffer.wrap(Bytes.toBytes(eventData.length)));
          fileChannel.write(ByteBuffer.wrap(eventData));
          log.info("TimedEvent of type " + timedEvent.getType() + " persisted!");
        } catch (IOException e) {
          throw new BufferException(e);
        }
      }
    }
  }

  @Override
  public List<TimedEvent> queryEvents(final QueryModel eventQueryModel) {
    return Lists.newArrayList();
  }

  public synchronized TimedEvent[] flip() {
    List<TimedEvent> result = Lists.newArrayList();
    try {
      fileChannel.position(0);
      drainFlag = Boolean.TRUE;
      ByteBuffer sizeByteBuffer = ByteBuffer.allocate(Integer.SIZE / 8);
      while (true) {
        sizeByteBuffer.clear();
        int readResult = fileChannel.read(sizeByteBuffer);
        if (readResult <= 0) {
          return result.toArray(new TimedEvent[result.size()]);
        }
        sizeByteBuffer.flip();
        Integer objectSize = sizeByteBuffer.getInt();
        ByteBuffer objectReadBuffer = ByteBuffer.allocate(objectSize);
        int objectReadResult = fileChannel.read(objectReadBuffer);
        if(objectReadResult <= 0) {
          throw new BufferException();
        }
        TimedEvent timedEvent = ObjectSerializer.deSerialize(objectReadBuffer, TimedEvent.class);
        result.add(timedEvent);
      }
    } catch (Exception e) {
      drainFlag = Boolean.FALSE;
      throw new BufferException(e);
    }
  }

  public synchronized void drain() {
    try {
      fileChannel.position(0);
      fileChannel.truncate(0);
      drainFlag = Boolean.FALSE;
    } catch (IOException e) {
      throw new BufferException(e);
    }
  }
}
