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

import java.util.HashMap;
import java.util.Map;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.apache.people.mreutegg.jsinfonia.util.CompositeItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerImpl;

public class CompositeItemManagerTest extends ItemManagerTestBase {

    private static final int ADDRESS_SPACE = 1024;

    private static final int ITEM_SIZE = 1024;

    private static final int NUM_MEMORY_NODE = 4;

    @Override
    protected ItemManager getItemManager(TransactionContext txContext) {
        Map<Integer, ItemManager> itemMgrs = new HashMap<>();
        for (int i = 0; i < NUM_MEMORY_NODE; i++) {
            itemMgrs.put(i, new ItemManagerImpl(txContext, new ItemReference(i, 0)));
        }
        return new CompositeItemManager(itemMgrs);
    }

    @Override
    protected ItemManager initializeItemManager(TransactionContext txContext) {
        Map<Integer, ItemManager> itemMgrs = new HashMap<>();
        for (int i = 0; i < NUM_MEMORY_NODE; i++) {
            ItemReference header = ItemManagerImpl.initialize(txContext, i, ADDRESS_SPACE);
            itemMgrs.put(i, new ItemManagerImpl(txContext, header));
        }
        return new CompositeItemManager(itemMgrs);
    }

    @Override
    protected ApplicationNode createApplicationNode() {
        SimpleMemoryNodeDirectory<InMemoryMemoryNode> directory = new SimpleMemoryNodeDirectory<>();
        for (int i = 0; i < NUM_MEMORY_NODE; i++) {
            directory.addMemoryNode(new InMemoryMemoryNode(i, ADDRESS_SPACE, ITEM_SIZE));
        }
        return new SimpleApplicationNode(directory, EXECUTOR);
    }

}
