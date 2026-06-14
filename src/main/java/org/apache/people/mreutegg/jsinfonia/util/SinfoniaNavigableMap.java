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
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import org.apache.people.mreutegg.jsinfonia.btree.BTree;

public class SinfoniaNavigableMap<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V> {

  private final BTree<K, V> btree;
  private final K fromKey;
  private final boolean fromInclusive;
  private final K toKey;
  private final boolean toInclusive;
  private final boolean descending;

  public SinfoniaNavigableMap(BTree<K, V> btree) {
    this(btree, null, true, null, true, false);
  }

  private SinfoniaNavigableMap(
      BTree<K, V> btree,
      K fromKey, boolean fromInclusive,
      K toKey, boolean toInclusive,
      boolean descending) {
    this.btree = btree;
    this.fromKey = fromKey;
    this.fromInclusive = fromInclusive;
    this.toKey = toKey;
    this.toInclusive = toInclusive;
    this.descending = descending;
  }

  @Override
  public int size() {
    int size = 0;
    Iterator<K> it = keyIterator();
    while (it.hasNext()) {
      it.next();
      size++;
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    return !keyIterator().hasNext();
  }

  @Override
  public V get(Object key) {
    @SuppressWarnings("unchecked")
    K k = (K) key;
    if (!inBounds(k)) {
      return null;
    }
    return btree.lookup(k);
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public V put(K key, V value) {
    if (!inBounds(key)) {
      throw new IllegalArgumentException("Key out of bounds");
    }
    V prev = btree.lookup(key);
    btree.insert(key, value);
    return prev;
  }

  @Override
  public V remove(Object key) {
    @SuppressWarnings("unchecked")
    K k = (K) key;
    if (!inBounds(k)) {
      return null;
    }
    V prev = btree.lookup(k);
    if (prev != null) {
      btree.delete(k);
    }
    return prev;
  }

  @Override
  public void clear() {
    Iterator<K> it = keyIterator();
    while (it.hasNext()) {
      K key = it.next();
      btree.delete(key);
    }
  }

  @Override
  public Comparator<? super K> comparator() {
    if (descending) {
      return java.util.Collections.reverseOrder(btree.comparator());
    }
    return btree.comparator();
  }

  @Override
  public K firstKey() {
    Map.Entry<K, V> entry = firstEntry();
    if (entry == null) {
      throw new NoSuchElementException();
    }
    return entry.getKey();
  }

  @Override
  public K lastKey() {
    Map.Entry<K, V> entry = lastEntry();
    if (entry == null) {
      throw new NoSuchElementException();
    }
    return entry.getKey();
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    return getBoundedEntry("lower", key);
  }

  @Override
  public K lowerKey(K key) {
    Map.Entry<K, V> entry = lowerEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    return getBoundedEntry("floor", key);
  }

  @Override
  public K floorKey(K key) {
    Map.Entry<K, V> entry = floorEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<K, V> ceilingEntry(K key) {
    return getBoundedEntry("ceiling", key);
  }

  @Override
  public K ceilingKey(K key) {
    Map.Entry<K, V> entry = ceilingEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<K, V> higherEntry(K key) {
    return getBoundedEntry("higher", key);
  }

  @Override
  public K higherKey(K key) {
    Map.Entry<K, V> entry = higherEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<K, V> firstEntry() {
    Iterator<Map.Entry<K, V>> it = entryIterator();
    return it.hasNext() ? it.next() : null;
  }

  @Override
  public Map.Entry<K, V> lastEntry() {
    Iterator<Map.Entry<K, V>> it = descendingEntryIterator();
    return it.hasNext() ? it.next() : null;
  }

  @Override
  public Map.Entry<K, V> pollFirstEntry() {
    Map.Entry<K, V> entry = firstEntry();
    if (entry != null) {
      btree.delete(entry.getKey());
    }
    return entry;
  }

  @Override
  public Map.Entry<K, V> pollLastEntry() {
    Map.Entry<K, V> entry = lastEntry();
    if (entry != null) {
      btree.delete(entry.getKey());
    }
    return entry;
  }

  @Override
  public NavigableMap<K, V> descendingMap() {
    return new SinfoniaNavigableMap<>(btree, fromKey, fromInclusive, toKey, toInclusive, !descending);
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    return new SinfoniaNavigableSet<>(this);
  }

  @Override
  public NavigableSet<K> keySet() {
    return navigableKeySet();
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    return descendingMap().navigableKeySet();
  }

  @Override
  public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    if (fromKey == null || toKey == null) {
      throw new NullPointerException();
    }
    if (comparator().compare(fromKey, toKey) > 0) {
      throw new IllegalArgumentException("fromKey > toKey");
    }
    return new SinfoniaNavigableMap<>(btree, fromKey, fromInclusive, toKey, toInclusive, descending);
  }

  @Override
  public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    if (toKey == null) {
      throw new NullPointerException();
    }
    return new SinfoniaNavigableMap<>(btree, fromKey, fromInclusive, toKey, inclusive, descending);
  }

  @Override
  public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    if (fromKey == null) {
      throw new NullPointerException();
    }
    return new SinfoniaNavigableMap<>(btree, fromKey, inclusive, toKey, toInclusive, descending);
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return new AbstractSet<>() {
      @Override
      public Iterator<Map.Entry<K, V>> iterator() {
        return entryIterator();
      }

      @Override
      public int size() {
        return SinfoniaNavigableMap.this.size();
      }
    };
  }

  // -------------------------------< internal >------------------------------

  Iterator<K> keyIterator() {
    K startKey = descending ? toKey : fromKey;
    boolean startInclusive = descending ? toInclusive : fromInclusive;
    K endKey = descending ? fromKey : toKey;
    boolean endInclusive = descending ? fromInclusive : toInclusive;
    return btree.keyIterator(startKey, startInclusive, endKey, endInclusive, descending);
  }

  private Iterator<Map.Entry<K, V>> entryIterator() {
    K startKey = descending ? toKey : fromKey;
    boolean startInclusive = descending ? toInclusive : fromInclusive;
    K endKey = descending ? fromKey : toKey;
    boolean endInclusive = descending ? fromInclusive : toInclusive;
    return btree.entryIterator(startKey, startInclusive, endKey, endInclusive, descending);
  }

  private Iterator<Map.Entry<K, V>> descendingEntryIterator() {
    K startKey = descending ? fromKey : toKey;
    boolean startInclusive = descending ? fromInclusive : toInclusive;
    K endKey = descending ? toKey : fromKey;
    boolean endInclusive = descending ? toInclusive : fromInclusive;
    return btree.entryIterator(startKey, startInclusive, endKey, endInclusive, !descending);
  }

  private boolean inBounds(K key) {
    if (key == null) {
      return false;
    }
    Comparator<? super K> comp = btree.comparator();
    if (fromKey != null) {
      int cmp = comp.compare(key, fromKey);
      if (fromInclusive ? cmp < 0 : cmp <= 0) {
        return false;
      }
    }
    if (toKey != null) {
      int cmp = comp.compare(key, toKey);
      if (toInclusive ? cmp > 0 : cmp >= 0) {
        return false;
      }
    }
    return true;
  }

  private boolean isBefore(K key) {
    Comparator<? super K> comp = btree.comparator();
    K minKey = descending ? toKey : fromKey;
    boolean minInclusive = descending ? toInclusive : fromInclusive;
    if (minKey == null) {
      return false;
    }
    int cmp = comp.compare(key, minKey);
    return minInclusive ? cmp < 0 : cmp <= 0;
  }

  private boolean isAfter(K key) {
    Comparator<? super K> comp = btree.comparator();
    K maxKey = descending ? fromKey : toKey;
    boolean maxInclusive = descending ? fromInclusive : toInclusive;
    if (maxKey == null) {
      return false;
    }
    int cmp = comp.compare(key, maxKey);
    return maxInclusive ? cmp > 0 : cmp >= 0;
  }

  private Map.Entry<K, V> getBoundedEntry(String type, K key) {
    if (isBefore(key)) {
      if (descending) {
        if ("floor".equals(type) || "lower".equals(type)) {
          return null;
        }
        // ceiling or higher -> first entry of descending map (underlying maxKey)
        return lastEntryUnderlying();
      } else {
        if ("floor".equals(type) || "lower".equals(type)) {
          return null;
        }
        // ceiling or higher -> first entry of map
        return firstEntryUnderlying();
      }
    }

    if (isAfter(key)) {
      if (descending) {
        if ("ceiling".equals(type) || "higher".equals(type)) {
          return null;
        }
        // floor or lower -> last entry of descending map (underlying minKey)
        return firstEntryUnderlying();
      } else {
        if ("ceiling".equals(type) || "higher".equals(type)) {
          return null;
        }
        // floor or lower -> last entry of map
        return lastEntryUnderlying();
      }
    }

    Map.Entry<K, V> candidate;
    if (descending) {
      switch (type) {
        case "floor":
          candidate = btree.ceilingEntry(key);
          break;
        case "ceiling":
          candidate = btree.floorEntry(key);
          break;
        case "lower":
          candidate = btree.higherEntry(key);
          break;
        case "higher":
          candidate = btree.lowerEntry(key);
          break;
        default:
          candidate = null;
      }
    } else {
      switch (type) {
        case "floor":
          candidate = btree.floorEntry(key);
          break;
        case "ceiling":
          candidate = btree.ceilingEntry(key);
          break;
        case "lower":
          candidate = btree.lowerEntry(key);
          break;
        case "higher":
          candidate = btree.higherEntry(key);
          break;
        default:
          candidate = null;
      }
    }

    if (candidate != null && inBounds(candidate.getKey())) {
      return candidate;
    }
    return null;
  }

  private Map.Entry<K, V> firstEntryUnderlying() {
    Iterator<Map.Entry<K, V>> it = btree.entryIterator(
        descending ? toKey : fromKey,
        descending ? toInclusive : fromInclusive,
        descending ? fromKey : toKey,
        descending ? fromInclusive : toInclusive,
        false);
    return it.hasNext() ? it.next() : null;
  }

  private Map.Entry<K, V> lastEntryUnderlying() {
    Iterator<Map.Entry<K, V>> it = btree.entryIterator(
        descending ? fromKey : toKey,
        descending ? fromInclusive : toInclusive,
        descending ? toKey : fromKey,
        descending ? toInclusive : fromInclusive,
        true);
    return it.hasNext() ? it.next() : null;
  }
}
