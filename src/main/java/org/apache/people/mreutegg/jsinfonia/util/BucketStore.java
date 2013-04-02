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

import java.util.List;

public interface BucketStore<K, V> {

	/**
	 * The initial number of buckets to allocate.
	 * 
	 * @return the initial number of buckets to allocate.
	 */
	public int getInitialNumberOfBuckets();
	
	public int getSplitIndex();
	
	public int incrementAndGetSplitIndex();
	
	public void resetSplitIndex();
	
	public int getLevel();
	
	public void incrementLevel();
	
	public int getSize();
	
	public List<Bucket<K, V>> getBucketList();
	
	/**
	 * Allocates / creates a new bucket.
	 * 
	 * @return a new bucket
	 */
	public Bucket<K, V> createBucket();

	/**
	 * Gets an existing bucket.
	 * 
	 * @param id
	 *            the bucket id.
	 * @return the bucket with the id or null if there is no existing bucket
	 *         with the given id.
	 */
	public Bucket<K, V> getBucket(BucketId id);

	/**
	 * Disposes the bucket with the given id.
	 * 
	 * @param id
	 *            the id of the bucket to dispose.
	 */
	public void disposeBucket(BucketId id);
}
