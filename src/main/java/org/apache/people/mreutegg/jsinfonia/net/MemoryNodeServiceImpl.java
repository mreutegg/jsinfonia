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
package org.apache.people.mreutegg.jsinfonia.net;

import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import org.apache.people.mreutegg.jsinfonia.thrift.MemoryNodeService;
import org.apache.people.mreutegg.jsinfonia.thrift.TItem;
import org.apache.people.mreutegg.jsinfonia.thrift.TItemReference;
import org.apache.people.mreutegg.jsinfonia.thrift.TMemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.thrift.TMiniTransaction;
import org.apache.people.mreutegg.jsinfonia.thrift.TResult;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.Vote;

public class MemoryNodeServiceImpl implements MemoryNodeService.Iface {

	private final MemoryNode memoryNode;
	
	public MemoryNodeServiceImpl(MemoryNode memoryNode) {
		this.memoryNode = memoryNode;
	}
	
	@Override
    public TMemoryNodeInfo getInfo() throws TException {
		TMemoryNodeInfo info = new TMemoryNodeInfo();
		info.setAddressSpace(memoryNode.getInfo().getAddressSpace());
		info.setId(memoryNode.getInfo().getId());
		info.setItemSize(memoryNode.getInfo().getItemSize());
	    return info;
    }

	@Override
	public TResult executeAndPrepare(TMiniTransaction tx, Set<Integer> memoryNodeIds)
			throws TException {
		MiniTransaction miniTx = Utils.convert(tx);
		Result r = memoryNode.executeAndPrepare(miniTx, memoryNodeIds);
		TResult result = Utils.convert(r);
		if (r.getVote() == Vote.OK) {
			List<Item> readItems = miniTx.getReadItems();
			for (int i = 0; i < readItems.size(); i++) {
				Item readItem = readItems.get(i);
				TItem item = new TItem();
				TItemReference ref = new TItemReference();
				ref.setMemoryNodeId(memoryNode.getInfo().getId());
				ref.setAddress(readItem.getAddress());
				ref.setOffset(readItem.getOffset());
				item.setReference(ref);
				item.setData(readItem.getData());
				result.addToReadItems(item);
			}
		}
		return result;
	}

	@Override
	public void commit(String txId, boolean commit) throws TException {
		memoryNode.commit(txId, commit);
	}
}
