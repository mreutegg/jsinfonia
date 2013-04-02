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
package org.apache.people.mreutegg.jsinfonia;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MiniTransaction {

	private final String txId;
	
	private final Set<Integer> memoryNodeIds = new HashSet<Integer>();
	
	private final List<Item> compareItems = new ArrayList<Item>();
	
	private final List<Item> readItems = new ArrayList<Item>();
	
	private final List<Item> writeItems = new ArrayList<Item>();
	
	public MiniTransaction(String txId) {
		this.txId = txId;
	}
	
	public void addCompareItem(Item item) {
		memoryNodeIds.add(item.getMemoryNodeId());
		compareItems.add(item);
	}
	
	public void addReadItem(Item item) {
		memoryNodeIds.add(item.getMemoryNodeId());
		readItems.add(item);
	}
	
	public void addWriteItem(Item item) {
		memoryNodeIds.add(item.getMemoryNodeId());
		writeItems.add(item);
	}

	public String getTxId() {
		return txId;
	}
	
	public Set<Integer> getMemoryNodeIds() {
		return memoryNodeIds;
	}
	
	public List<Item> getCompareItems() {
		return compareItems;
	}
	
	public List<Item> getReadItems() {
		return readItems;
	}
	
	public List<Item> getWriteItems() {
		return writeItems;
	}
	
	public MiniTransaction getTransactionForMemoryNode(int id) {
		if (!memoryNodeIds.contains(id)) {
			throw new IllegalArgumentException("unknown transaction id " + id);
		}

		// optimize single node case
		if (memoryNodeIds.size() == 1) {
			return this;
		}
		
		MiniTransaction mt = new MiniTransaction(txId);
		for (Item compareItem : compareItems) {
			if (compareItem.getMemoryNodeId() == id) {
				mt.addCompareItem(compareItem);
			}
		}
		for (Item readItem : readItems) {
			if (readItem.getMemoryNodeId() == id) {
				mt.addReadItem(readItem);
			}
		}
		for (Item writeItem : writeItems) {
			if (writeItem.getMemoryNodeId() == id) {
				mt.addWriteItem(writeItem);
			}
		}
		return mt;
	}
	
}
