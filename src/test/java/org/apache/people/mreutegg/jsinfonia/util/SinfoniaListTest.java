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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.fs.FileMemoryNode;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.junit.Before;
import org.junit.Test;

public class SinfoniaListTest {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    private static final int NUM_MEMORY_NODES = 4;
    private static final int ADDRESS_SPACE = 1024;
    private static final int ITEM_SIZE = 1024;
    private ApplicationNode appNode;
    private TransactionManager txMgr;
    private ItemManagerFactory itemMgrFactory;


    @Before
    public void init() {
        SimpleMemoryNodeDirectory<MemoryNode> directory = new SimpleMemoryNodeDirectory<MemoryNode>();
        for (int i = 0; i < NUM_MEMORY_NODES; i++) {
            directory.addMemoryNode(new InMemoryMemoryNode(i, ADDRESS_SPACE, ITEM_SIZE));
        }
        appNode = new SimpleApplicationNode(directory, EXECUTOR);
        txMgr = createTransactionManager();
        // initialize item manager & factory
        final List<ItemReference> itemMgrHeaderRefs = txMgr.execute(
                new Transaction<List<ItemReference>>() {
            @Override
            public List<ItemReference> perform(TransactionContext txContext) {
                List<ItemReference> headerRefs = new ArrayList<ItemReference>();
                for (int i = 0; i < NUM_MEMORY_NODES; i++) {
                    headerRefs.add(ItemManagerImpl.initialize(txContext, i, ADDRESS_SPACE));
                }
                return headerRefs;
            }
        });
        itemMgrFactory = new ItemManagerFactory() {
            @Override
            public ItemManager createItemManager(TransactionContext txContext) {
                Map<Integer, ItemManager> itemMgrs = new HashMap<Integer, ItemManager>();
                for (ItemReference r : itemMgrHeaderRefs) {
                    itemMgrs.put(r.getMemoryNodeId(), new ItemManagerImpl(txContext, r));
                }
                return new CompositeItemManager(itemMgrs);
            }
        };
    }

    @Test
    public void size() {
        List<Integer> list = createList();
        assertEquals(0, list.size());
    }

    @Test
    public void add() {
        List<Integer> list = createList();
        int numValues = 1000;
        for (int i = 0; i < numValues; i++) {
            list.add(i);
        }
        assertEquals(numValues, list.size());
    }

    @Test
    public void iterator() {
        List<Integer> list = createList();
        int numValues = 1000;
        for (int i = 0; i < numValues; i++) {
            list.add(i);
        }
        int i = 0;
        for (Integer value : list) {
            assertEquals(i++, value.intValue());
        }
    }

    private List<Integer> createList() {
        return txMgr.execute(new Transaction<List<Integer>>() {
            @Override
            public List<Integer> perform(TransactionContext txContext) {
                ItemReference ref = itemMgrFactory.createItemManager(txContext).alloc();
                return SinfoniaList.newList(ref, txContext, itemMgrFactory,
                        new BucketReader<Integer>() {
                            @Override
                            public Iterable<Integer> read(ByteBuffer data) {
                                char num = data.getChar();
                                List<Integer> items = new ArrayList<Integer>();
                                while (num-- > 0) {
                                    items.add(data.getInt());
                                }
                                return items;
                            }
                        },
                        new BucketWriter<Integer>() {
                            @Override
                            public int write(Iterable<Integer> entries,
                                    ByteBuffer data) {
                                char num = 0;
                                data.putChar(num);
                                try {
                                    for (Integer i : entries) {
                                        data.putInt(i);
                                        num++;
                                    }
                                } catch (BufferOverflowException e) {
                                    // stop writing more entries
                                }
                                data.putChar(0, num);
                                return num;
                            }
                        });
            }
        });
    }

    private TransactionManager createTransactionManager() {
        return new TransactionManager(appNode, new DataItemCache(128));
    }
}
