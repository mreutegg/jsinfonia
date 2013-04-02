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
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.people.mreutegg.jsinfonia.ItemReference;

/**
 * 
 * TODO: introduce strategy to pick a memory node for allocation?
 */
public class CompositeItemManager implements ItemManager {

	private final Map<Integer, ItemManager> itemManagers = new HashMap<Integer, ItemManager>();

	private final Random random = new Random(0);
	
	public CompositeItemManager(Map<Integer, ItemManager> itemManagers) {
		this.itemManagers.putAll(itemManagers);
	}
	
	@Override
	public ItemReference alloc() {
		List<Integer> memoryNodeIds = new ArrayList<Integer>();
		memoryNodeIds.addAll(itemManagers.keySet());
		while (!memoryNodeIds.isEmpty()) {
			int idx = random.nextInt(memoryNodeIds.size());
			Integer memoryNodeId = memoryNodeIds.get(idx);
			ItemManager itemMgr = itemManagers.get(memoryNodeId);
			ItemReference ref = itemMgr.alloc();
			if (ref != null) {
				return ref;
			} else {
				memoryNodeIds.remove(idx);
			}
		}
		return null;
	}

	@Override
	public void free(ItemReference ref) {
		ItemManager itemMgr = itemManagers.get(ref.getMemoryNodeId());
		if (itemMgr == null) {
			throw new IllegalArgumentException();
		}
		itemMgr.free(ref);
	}

}
