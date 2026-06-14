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
package org.apache.people.mreutegg.jsinfonia.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;

public class SinfoniaHashMap<K, V> extends AbstractMap<K, V> {

  private final TransactionManager txManager;
  private final ItemManagerFactory factory;
  private final ItemReference headerRef;
  private final BucketReader<Entry<K, V>> reader;
  private final BucketWriter<Entry<K, V>> writer;

  private final ThreadLocal<TransactionContext> currentTxContext = new ThreadLocal<>();

  private final TransactionContext activeTxContext = new TransactionContext() {
    @Override
    public <T> T read(ItemReference reference, DataOperation<T> op) {
      TransactionContext ctx = currentTxContext.get();
      if (ctx == null) {
        ctx = txManager;
      }
      return ctx.read(reference, op);
    }

    @Override
    public <T> T write(ItemReference reference, DataOperation<T> op) {
      TransactionContext ctx = currentTxContext.get();
      if (ctx == null) {
        ctx = txManager;
      }
      return ctx.write(reference, op);
    }
  };

  private volatile Map<K, V> map;

  public SinfoniaHashMap(
      TransactionManager txManager,
      ItemManagerFactory factory,
      ItemReference headerRef,
      BucketReader<Entry<K, V>> reader,
      BucketWriter<Entry<K, V>> writer) {
    this.txManager = txManager;
    this.factory = factory;
    this.headerRef = headerRef;
    this.reader = reader;
    this.writer = writer;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return txManager.execute(
        txContext -> {
          currentTxContext.set(txContext);
          try {
            return getMap().entrySet();
          } finally {
            currentTxContext.remove();
          }
        });
  }

  @Override
  public V put(final K key, final V value) {
    if (key == null) {
      throw new NullPointerException("key must not be null");
    }
    if (value == null) {
      throw new NullPointerException("value must not be null");
    }
    return txManager.execute(
        txContext -> {
          currentTxContext.set(txContext);
          try {
            return getMap().put(key, value);
          } finally {
            currentTxContext.remove();
          }
        });
  }

  @Override
  public V remove(final Object key) {
    return txManager.execute(
        txContext -> {
          currentTxContext.set(txContext);
          try {
            return getMap().remove(key);
          } finally {
            currentTxContext.remove();
          }
        });
  }

  @Override
  public V get(final Object key) {
    return txManager.execute(
        txContext -> {
          currentTxContext.set(txContext);
          try {
            return getMap().get(key);
          } finally {
            currentTxContext.remove();
          }
        });
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    txManager.execute(
        txContext -> {
          currentTxContext.set(txContext);
          try {
            getMap().putAll(m);
            return null;
          } finally {
            currentTxContext.remove();
          }
        });
  }

  // -------------------------------< internal >------------------------------

  private Map<K, V> getMap() {
    Map<K, V> m = map;
    if (m == null) {
      synchronized (this) {
        m = map;
        if (m == null) {
          SinfoniaBucketStore<K, V> store =
              new SinfoniaBucketStore<>(
                  factory.createItemManager(activeTxContext), activeTxContext, headerRef, reader, writer);
          m = map = new LinearHashMap<>(store);
        }
      }
    }
    return m;
  }
}
