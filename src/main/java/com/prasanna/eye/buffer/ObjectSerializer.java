package com.prasanna.eye.buffer;

import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.prasanna.eye.model.TimedEvent;

public class ObjectSerializer {
  // need to use thread local
  private final static Kryo kryo;
  static {
    kryo = new Kryo();
    kryo.register(TimedEvent.class, new FieldSerializer(kryo, TimedEvent.class));
  }

  public static byte[] serialize(Object object) {
    Output output = new Output(128, 1024 * 10);
    kryo.writeObject(output, object);
    return output.toBytes();
  }

  public static <T> T deSerialize(final ByteBuffer objectReadBuffer, final Class<T> objectClass) {
    Input input = new Input(objectReadBuffer.array());
    return kryo.readObject(input, objectClass);

  }
}
