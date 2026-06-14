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

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public class SinfoniaNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E> {

  private final NavigableMap<E, ?> map;

  public SinfoniaNavigableSet(NavigableMap<E, ?> map) {
    this.map = map;
  }

  @Override
  public E lower(E e) {
    return map.lowerKey(e);
  }

  @Override
  public E floor(E e) {
    return map.floorKey(e);
  }

  @Override
  public E ceiling(E e) {
    return map.ceilingKey(e);
  }

  @Override
  public E higher(E e) {
    return map.higherKey(e);
  }

  @Override
  public E pollFirst() {
    Map.Entry<E, ?> entry = map.pollFirstEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public E pollLast() {
    Map.Entry<E, ?> entry = map.pollLastEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Iterator<E> iterator() {
    if (map instanceof SinfoniaNavigableMap) {
      return ((SinfoniaNavigableMap<E, ?>) map).keyIterator();
    }
    final Iterator<? extends Map.Entry<E, ?>> entryIt = map.entrySet().iterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return entryIt.hasNext();
      }
      @Override
      public E next() {
        return entryIt.next().getKey();
      }
      @Override
      public void remove() {
        entryIt.remove();
      }
    };
  }

  @Override
  public NavigableSet<E> descendingSet() {
    return new SinfoniaNavigableSet<>(map.descendingMap());
  }

  @Override
  public Iterator<E> descendingIterator() {
    return descendingSet().iterator();
  }

  @Override
  public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return new SinfoniaNavigableSet<>(map.subMap(fromElement, fromInclusive, toElement, toInclusive));
  }

  @Override
  public NavigableSet<E> headSet(E toElement, boolean inclusive) {
    return new SinfoniaNavigableSet<>(map.headMap(toElement, inclusive));
  }

  @Override
  public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return new SinfoniaNavigableSet<>(map.tailMap(fromElement, inclusive));
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return subSet(fromElement, true, toElement, false);
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    return headSet(toElement, false);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public Comparator<? super E> comparator() {
    return map.comparator();
  }

  @Override
  public E first() {
    return map.firstKey();
  }

  @Override
  public E last() {
    return map.lastKey();
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public boolean remove(Object o) {
    return map.remove(o) != null;
  }

  @Override
  public void clear() {
    map.clear();
  }
}
