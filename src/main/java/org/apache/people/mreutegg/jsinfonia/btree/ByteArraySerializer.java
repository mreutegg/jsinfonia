/*
 * Copyright 2013 Marcel Reutegger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.people.mreutegg.jsinfonia.btree;

import java.nio.ByteBuffer;

public class ByteArraySerializer implements Serializer<byte[]> {

  public static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

  private ByteArraySerializer() {}

  @Override
  public void serialize(ByteBuffer buffer, byte[] bytes) {
    if (bytes == null) {
      buffer.putInt(-1);
    } else {
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    }
  }

  @Override
  public byte[] deserialize(ByteBuffer buffer) {
    int length = buffer.getInt();
    if (length == -1) {
      return null;
    }
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }
}
