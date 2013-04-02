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

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerImpl;

public class ItemManagerImplTest extends ItemManagerTestBase {

	private static final int ADDRESS_SPACE = 1024;
	
	private static final int ITEM_SIZE = 1024;
	
	@Override
	protected ItemManager getItemManager(TransactionContext txContext) {
		return new ItemManagerImpl(txContext, new ItemReference(0, 0));
	}

	@Override
	protected ApplicationNode createApplicationNode() {
		SimpleMemoryNodeDirectory<InMemoryMemoryNode> directory = new SimpleMemoryNodeDirectory<InMemoryMemoryNode>();
		directory.addMemoryNode(new InMemoryMemoryNode(0, ADDRESS_SPACE, ITEM_SIZE));
		return new SimpleApplicationNode(directory, EXECUTOR);
	}

	@Override
	protected ItemManager initializeItemManager(TransactionContext txContext) {
		ItemReference header = ItemManagerImpl.initialize(txContext, 0, ADDRESS_SPACE);
		return new ItemManagerImpl(txContext, header);
	}

}
