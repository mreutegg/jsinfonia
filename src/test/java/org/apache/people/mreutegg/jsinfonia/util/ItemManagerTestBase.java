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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public abstract class ItemManagerTestBase extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(ItemManagerTestBase.class);

    protected static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    protected abstract ItemManager getItemManager(TransactionContext txContext);

    protected abstract ItemManager initializeItemManager(TransactionContext txContext);

    protected abstract ApplicationNode createApplicationNode();

    public void testAllocFree() throws Exception {
        final ApplicationNode appNode = createApplicationNode();
        TransactionManager txMgr = new TransactionManager(
                appNode, new DataItemCache(128));

        txMgr.execute(new Transaction<ItemManager>() {
            @Override
            public ItemManager perform(TransactionContext txContext) {
                return initializeItemManager(txContext);
            }
        });

        final List<ItemReference> refs = txMgr.execute(new Transaction<List<ItemReference>>() {
            @Override
            public List<ItemReference> perform(TransactionContext txContext) {
                ItemManager itemManager = getItemManager(txContext);
                List<ItemReference> refs = new ArrayList<ItemReference>();
                ItemReference ref;
                while ((ref = itemManager.alloc()) != null) {
                    refs.add(ref);
                }
                return refs;
            }
        });
        int numRefs = refs.size();
        // free all
        txMgr.execute(new Transaction<Void>() {
            @Override
            public Void perform(TransactionContext txContext) {
                ItemManager itemManager = getItemManager(txContext);
                for (ItemReference ref : refs) {
                    itemManager.free(ref);
                }
                return null;
            }
        });

        final List<Integer> allocs = Collections.synchronizedList(new ArrayList<Integer>());
        List<Thread> workers = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            workers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    TransactionManager txMgr = new TransactionManager(
                            appNode, new DataItemCache(128));
                    int numAllocs = 0;
                    ItemReference ref;
                    while ((ref = txMgr.execute(new Transaction<ItemReference>() {
                            @Override
                            public ItemReference perform(TransactionContext txContext) {
                                ItemManager itemManager = getItemManager(txContext);
                                return itemManager.alloc();
                            }
                        })) != null) {
                        numAllocs++;
                        log.debug("alloc() : " + ref);
                    }
                    allocs.add(numAllocs);
                }
            }));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        int numAllocs = 0;
        for (int a : allocs) {
            numAllocs += a;
        }
        assertEquals(numRefs, numAllocs);

        // perform random free / alloc
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            final ItemReference ref = refs.get(random.nextInt(refs.size()));
            txMgr.execute(new Transaction<Void>() {
                @Override
                public Void perform(TransactionContext txContext) {
                    ItemManager itemManager = getItemManager(txContext);
                    itemManager.free(ref);
                    return null;
                }
            });
            ItemReference allocRef = txMgr.execute(new Transaction<ItemReference>() {
                @Override
                public ItemReference perform(TransactionContext txContext) {
                    ItemManager itemManager = getItemManager(txContext);
                    return itemManager.alloc();
                }
            });
            log.debug("free/alloc: " + ref);
            assertEquals(ref, allocRef);
        }
    }
}
