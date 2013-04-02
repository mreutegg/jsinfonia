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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class LinearHashMap<K, V> extends AbstractMap<K, V> {

	private final Set<Map.Entry<K, V>> entries = new EntrySet();
	
	private final BucketStore<K, V> bucketStore;
	
	public LinearHashMap() {
		this(10);
	}
	
	public LinearHashMap(int entriesPerBucket) {
		this(new MemoryBucketStore<K, V>(entriesPerBucket));
	}
	
	public LinearHashMap(BucketStore<K, V> bucketStore) {
		this.bucketStore = bucketStore;
	}
	
	@Override
    public Set<Map.Entry<K, V>> entrySet() {
		return entries;
    }
	
	@Override
    public V put(K key, V value) {
		if (value == null) {
			throw new NullPointerException("value must not be null");
		}
		if (key == null) {
			throw new NullPointerException("key must not be null");
		}
		Bucket<K, V> b = bucketStore.getBucketList().get(getBucketIndex(key));
		V retVal = b.put(key, value);
		if (b.isOverflowed()) {
			// add bucket and redistribute at p
			b = bucketStore.getBucketList().get(bucketStore.getSplitIndex());
			Map<K, V> redistribute = new HashMap<K, V>();
			b.transferTo(redistribute);
			bucketStore.getBucketList().add(bucketStore.createBucket());
			if (bucketStore.incrementAndGetSplitIndex() >= (bucketStore.getInitialNumberOfBuckets() << bucketStore.getLevel())) {
				bucketStore.incrementLevel();
				bucketStore.resetSplitIndex();
			}
			for (Map.Entry<K, V> e : redistribute.entrySet()) {
				b = bucketStore.getBucketList().get(getBucketIndex(e.getKey()));
				b.put(e.getKey(), e.getValue());
			}
		}
		return retVal;
    }
	
	@Override
    public V remove(Object key) {
		Bucket<K, V> b = bucketStore.getBucketList().get(getBucketIndex(key));
		return b.remove(key);
    }

	@Override
    public V get(Object key) {
		Bucket<K, V> b = bucketStore.getBucketList().get(getBucketIndex(key));
		return b.get(key);
    }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("LinearHashMap(").append(bucketStore).append(") level: ");
		sb.append(bucketStore.getLevel()).append("\n");
		for (int i = 0; i < bucketStore.getBucketList().size(); i++) {
			if (i == bucketStore.getSplitIndex()) {
				sb.append("=>");
			} else {
				sb.append("  ");
			}
			sb.append(bucketStore.getBucketList().get(i).toString()).append("\n");
		}
		return sb.toString();
	}

	//----------------------------< internal >---------------------------------

	private int getBucketIndex(Object key) {
		int h = Math.abs(key.hashCode());
		int idx = h % (bucketStore.getInitialNumberOfBuckets() << bucketStore.getLevel());
		if (idx < bucketStore.getSplitIndex()) {
			idx = h % (bucketStore.getInitialNumberOfBuckets() << (bucketStore.getLevel() + 1));
		}
		return idx;
	}
	
	private class EntrySet extends AbstractSet<Map.Entry<K, V>> {

		@Override
        public Iterator<Map.Entry<K, V>> iterator() {
			return new Iterator<Map.Entry<K,V>>() {

				private int bucketIndex = 0;
				
				private Bucket<K, V> currentBucket = bucketStore.getBucketList().get(bucketIndex);
				
				private Iterator<K> currentIterator =
						currentBucket.getKeys().iterator();
				
				private Map.Entry<K, V> next = null;
				
				{
					fetchNext();
				}
				
				@Override
                public boolean hasNext() {
					return next != null;
                }

				@Override
                public Map.Entry<K, V> next() {
					if (next == null) {
						throw new NoSuchElementException();
					}
					Map.Entry<K, V> n = next;
					fetchNext();
					return n;
                }

				@Override
                public void remove() {
					throw new UnsupportedOperationException("remove");
                }

				private void fetchNext() {
					while (!currentIterator.hasNext()) {
						// check next in list
						if (++bucketIndex < bucketStore.getBucketList().size()) {
							currentBucket = bucketStore.getBucketList().get(bucketIndex);
							currentIterator = currentBucket.getKeys().iterator();
						} else {
							// no more buckets
							next = null;
							return;
						}
					}
					K key = currentIterator.next();
					next = new Entry(key, currentBucket.get(key));
                }
				
			};
        }

		@Override
        public int size() {
			return bucketStore.getSize();
        }
	}

	private class Entry extends AbstractMap.SimpleEntry<K, V> {

		private static final long serialVersionUID = -8389916147101698704L;

		public Entry(K key, V value) {
			super(key, value);
		}
		
	}
}
