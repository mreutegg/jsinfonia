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
import java.util.ArrayList;
import java.util.List;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

public abstract class BTreeNode<K, V> {

  public static final byte TYPE_INTERNAL = 0;
  public static final byte TYPE_LEAF = 1;

  protected final TransactionContext txContext;
  protected final ItemReference ref;
  protected final Serializer<K> keySerializer;
  protected final Serializer<V> valueSerializer;
  protected final List<K> keys = new ArrayList<>();

  protected BTreeNode(
      TransactionContext txContext,
      ItemReference ref,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.txContext = txContext;
    this.ref = ref;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public ItemReference getReference() {
    return ref;
  }

  public int getKeyCount() {
    return keys.size();
  }

  public K getKey(int index) {
    return keys.get(index);
  }

  public abstract void save();

  public abstract void load();

  public static <K, V> BTreeNode<K, V> load(
      final TransactionContext txContext,
      final ItemReference ref,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer) {
    return txContext.read(
        ref,
        data -> {
          byte type = data.get();
          data.rewind();
          BTreeNode<K, V> node;
          if (type == TYPE_INTERNAL) {
            node = new InternalNode<>(txContext, ref, keySerializer, valueSerializer);
          } else if (type == TYPE_LEAF) {
            node = new LeafNode<>(txContext, ref, keySerializer, valueSerializer);
          } else {
            throw new IllegalStateException("Unknown node type: " + type);
          }
          node.load();
          return node;
        });
  }

  protected void writeKey(ByteBuffer buffer, K key) {
    keySerializer.serialize(buffer, key);
  }

  protected K readKey(ByteBuffer buffer) {
    return keySerializer.deserialize(buffer);
  }

  protected void writeValue(ByteBuffer buffer, V value) {
    valueSerializer.serialize(buffer, value);
  }

  protected V readValue(ByteBuffer buffer) {
    return valueSerializer.deserialize(buffer);
  }

  protected void writeItemReference(ByteBuffer buffer, ItemReference ref) {
    buffer.putInt(ref.getMemoryNodeId());
    buffer.putInt(ref.getAddress());
  }

  protected ItemReference readItemReference(ByteBuffer buffer) {
    int memoryNodeId = buffer.getInt();
    int address = buffer.getInt();
    return new ItemReference(memoryNodeId, address);
  }
}
