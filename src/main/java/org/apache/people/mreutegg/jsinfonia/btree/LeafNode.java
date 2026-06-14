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

import java.util.ArrayList;
import java.util.List;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

public class LeafNode<K, V> extends BTreeNode<K, V> {

  protected final List<V> values = new ArrayList<>();

  public LeafNode(
      TransactionContext txContext,
      ItemReference ref,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    super(txContext, ref, keySerializer, valueSerializer);
  }

  public V getValue(int index) {
    return values.get(index);
  }

  public void addEntry(int index, K key, V value) {
    keys.add(index, key);
    values.add(index, value);
  }

  public void removeEntry(int index) {
    keys.remove(index);
    values.remove(index);
  }

  public void updateValue(int index, V value) {
    values.set(index, value);
  }

  @Override
  public void save() {
    txContext.write(
        ref,
        data -> {
          data.put(TYPE_LEAF);
          data.putInt(keys.size());
          for (K key : keys) {
            writeKey(data, key);
          }
          for (V value : values) {
            writeValue(data, value);
          }
          return null;
        });
  }

  @Override
  public void load() {
    txContext.read(
        ref,
        data -> {
          byte type = data.get();
          if (type != TYPE_LEAF) {
            throw new IllegalStateException("Not a leaf node");
          }
          int count = data.getInt();
          keys.clear();
          for (int i = 0; i < count; i++) {
            keys.add(readKey(data));
          }
          values.clear();
          for (int i = 0; i < count; i++) {
            values.add(readValue(data));
          }
          return null;
        });
  }
}
