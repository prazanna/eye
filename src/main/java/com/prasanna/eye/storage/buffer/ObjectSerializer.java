package com.prasanna.eye.storage.buffer;

import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.prasanna.eye.http.model.TimedEvent;

public class ObjectSerializer {
  // need to use thread local
  private final static ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue()
    {
      Kryo kryo = new Kryo();
      kryo.register(TimedEvent.class, new FieldSerializer(kryo, TimedEvent.class));
      return kryo;
    }
  };

  public static byte[] serialize(Object object) {
    Output output = new Output(128, 1024 * 10);
    kryo.get().writeObject(output, object);
    return output.toBytes();
  }

  public static <T> T deSerialize(final ByteBuffer objectReadBuffer, final Class<T> objectClass) {
    Input input = new Input(objectReadBuffer.array());
    return kryo.get().readObject(input, objectClass);

  }
}
