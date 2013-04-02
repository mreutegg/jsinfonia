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
package org.apache.people.mreutegg.jsinfonia.data;

import org.apache.people.mreutegg.jsinfonia.ItemReference;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


public class DataItemCache {

	private final Cache<ItemReference, DataItem> items;
	
	public DataItemCache() {
		this(Integer.MAX_VALUE);
	}
	
	public DataItemCache(int maxSize) {
		items = CacheBuilder.newBuilder().maximumSize(maxSize).build();
	}
	
	public DataItem getItem(ItemReference ref) {
		return items.getIfPresent(ref);
	}
	
	public DataItem getItem(int memoryNodeId, int address) {
		return getItem(new ItemReference(memoryNodeId, address));
	}
	
	public void putItem(ItemReference ref, DataItem item) {
		items.put(ref, item);
	}
	
	public void putItem(int memoryNodeId, int address, DataItem item) {
		putItem(new ItemReference(memoryNodeId, address), item);
	}
	
	public Iterable<ItemReference> getKeys() {
		return items.asMap().keySet();
	}
	
	public void clear() {
		items.invalidateAll();
	}

	public void evict(Iterable<ItemReference> references) {
		items.invalidateAll(references);
	}
	
}
