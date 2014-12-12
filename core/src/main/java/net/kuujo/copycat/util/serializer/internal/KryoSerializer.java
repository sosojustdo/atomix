/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.util.serializer.internal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;

/**
 * Kryo serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KryoSerializer implements Serializer {
  private final Kryo kryo = new Kryo();
  private final ByteBufferInput input = new ByteBufferInput();
  private final ByteBufferOutput output = new ByteBufferOutput();

  @Override
  @SuppressWarnings("unchecked")
  public <T> T readObject(ByteBuffer buffer) {
    input.setBuffer(buffer);
    return (T) kryo.readClassAndObject(input);
  }

  @Override
  public <T> ByteBuffer writeObject(T object) {
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    output.setBuffer(buffer);
    kryo.writeClassAndObject(output, object);
    buffer.rewind();
    return buffer.compact();
  }

}
