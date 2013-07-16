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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class MemoryBucketStore<K, V> implements BucketStore<K, V> {

	/**
	 * Initial number of buckets
	 */
	private static final int INITIAL_BUCKET_SIZE = 4;
	
	private final Map<BucketId, MapBucket<K, V>> store = new IdentityHashMap<BucketId, MapBucket<K,V>>();
	
	private final int entriesPerBucket;
	
	/**
	 * Pointer to bucket where next split occurs
	 */
	private int splitIndex = 0;
	
	/**
	 * The current level
	 */
	private int level = 0;
	
	/**
	 * Number of items in this bucket store
	 */
	private int size = 0;
	
	/**
	 * The primary buckets list
	 */
	private final List<MapBucket<K, V>> buckets = new ArrayList<MapBucket<K, V>>();
	
	public MemoryBucketStore(int entriesPerBucket) {
		this.entriesPerBucket = entriesPerBucket;
		// initialize buckets
		for (int i = 0; i < getInitialNumberOfBuckets(); i++) {
			buckets.add(createBucket());
		}
	}
	
	@Override
	public int getSplitIndex() {
		return splitIndex;
	}

	@Override
	public int incrementAndGetSplitIndex() {
		return ++splitIndex;
	}

	@Override
	public void resetSplitIndex() {
		splitIndex = 0;
	}

	@Override
	public int getLevel() {
		return level;
	}

	@Override
	public void incrementLevel() {
		level++;
	}

	@Override
	public int getInitialNumberOfBuckets() {
		return INITIAL_BUCKET_SIZE;
	}

	@Override
	public int getSize() {
		return size;
	}

	@Override
	public List<MapBucket<K, V>> getBucketList() {
		return buckets;
	}

	@Override
	public MapBucket<K, V> createBucket() {
		return createBucketInternal();
	}

	@Override
	public MapBucket<K, V> getBucket(BucketId id) {
		return store.get(id);
	}

	@Override
	public void disposeBucket(BucketId id) {
		store.remove(id);
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + entriesPerBucket + ")";
	}
	
	//------------------------------< internal >-------------------------------

	private MemoryBucket createBucketInternal() {
		MemoryBucket bucket = new MemoryBucket();
		store.put(bucket, bucket);
		return bucket;
	}
	
	//---------------------------< BucketImpl >--------------------------------

	private class MemoryBucket implements MapBucket<K, V>, BucketId  {
		
		private final Map<K, V> entries = new HashMap<K, V>();
		
		@Override
        public BucketId getId() {
	        return this;
        }
		
		@Override
		public V get(Object key) {
			return entries.get(key);
		}

		@Override
		public V put(K key, V value) {
			V previous = entries.put(key, value);
			if (previous == null) {
				MemoryBucketStore.this.size++;
			}
			return previous;
		}

		@Override
		public V remove(Object key) {
			V value = entries.remove(key);
			if (value != null) {
				MemoryBucketStore.this.size--;
			}
			return value;
		}
		
		@Override
		public Iterable<K> getKeys() {
			return entries.keySet();
		}
		
		public boolean isOverflowed() {
			return entries.size() > entriesPerBucket;
		}

		@Override
		public void transferTo(Map<K, V> toDistribute) {
			toDistribute.putAll(entries);
			MemoryBucketStore.this.size -= getSize();
	        entries.clear();
		}
		
		@Override
		public int getSize() {
			return entries.size();
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(entries);
			return sb.toString();
		}
	}
}

