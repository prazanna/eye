package com.prasanna.eye.buffer;

import org.apache.commons.collections.BufferOverflowException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import com.google.common.collect.Lists;
import com.prasanna.eye.model.TimedEvent;

@Component
@Scope("prototype")
public class TimedEventBuffer implements EventBuffer<TimedEvent> {
  private volatile Boolean drainFlag = Boolean.FALSE;
  private FileChannel fileChannel;
  private Logger log = LoggerFactory.getLogger(TimedEventBuffer.class);
  File bufferFile;

  @PostConstruct
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

  @PreDestroy
  private void cleanup() {
    if(bufferFile.exists()) {
      //noinspection ResultOfMethodCallIgnored
      bufferFile.delete();
    }
  }

  @Override
  public Boolean getDrainFlag() {
    return drainFlag;
  }

  @Override
  public void offer(final TimedEvent timedEvent) {
    if (drainFlag) {
      throw new BufferOverflowException();
    }

    synchronized (this) {
      if (drainFlag) {
        throw new BufferOverflowException();
      }
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

  @Override
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

  @Override
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
