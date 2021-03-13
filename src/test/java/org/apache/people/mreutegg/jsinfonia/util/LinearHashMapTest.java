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

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.fs.FileMemoryNode;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.apache.people.mreutegg.jsinfonia.util.BucketReader;
import org.apache.people.mreutegg.jsinfonia.util.BucketWriter;
import org.apache.people.mreutegg.jsinfonia.util.CompositeItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerFactory;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerImpl;
import org.apache.people.mreutegg.jsinfonia.util.LinearHashMap;
import org.apache.people.mreutegg.jsinfonia.util.SinfoniaHashMap;

import junit.framework.TestCase;

public class LinearHashMapTest extends TestCase {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    private int addressSpace;
    private int itemSize;
    private int numMemoryNodes;
    private boolean useFileMemoryNode;
    private ApplicationNode appNode;
    private MemoryNodeDirectory<? extends MemoryNode> dir;
    private ItemReference hashMapHeaderRef;

    public void setUp() {
        addressSpace = 1024;
        itemSize = 1024;
        numMemoryNodes = 4;
        useFileMemoryNode = false;
    }

    public void tearDown() {
        if (dir != null) {
            for (Integer memoryNodeId : dir.getMemoryNodeIds()) {
                MemoryNode memoryNode = dir.getMemoryNode(memoryNodeId);
                if (memoryNode instanceof FileMemoryNode) {
                    try {
                        ((FileMemoryNode) memoryNode).close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        appNode = null;
        hashMapHeaderRef = null;
    }

    public void testMap() {
        Map<Integer, Integer> map = new LinearHashMap<>(20);
        System.out.println(map);

        for (int j = 0; j < 100; j++) {
            int numEntries = (int) (Math.random() * 200.0);
            int size = 0;
            for (int i = 0; i < numEntries; i++) {
                map.put(i, i);
                size++;
                //System.out.println(map);
                //System.out.println("get(" + i + ") : " + map.get(i));
                assertEquals((Integer) i, (Integer) map.get(i));
                assertEquals(size, map.size());
            }
            for (int i = 0; i < numEntries; i++) {
                //System.out.println("remove(" + i + ") : " + map.remove(i));
                //System.out.println(map);
                assertEquals((Integer) i, (Integer) map.remove(i));
                size--;
                assertEquals(size, map.size());
            }
        }
    }

    public void testPerformanceHashMap() throws IOException {
        int numKeys = 10 * 1000;

        System.out.println("HashMap:       " + runRandom(
                new HashMap<Integer, Integer>(), numKeys));

        System.out.println("LinearHashMap: " + runRandom(
                new LinearHashMap<Integer, Integer>(20), numKeys));

        System.out.println("SinfoniaHashMap: " + runRandom(
                initializeSinfoniaHashMap(), numKeys));
    }

    public void testConcurrentSinfoniaHashMap() throws Exception {
        doConcurrentSinfoniaHashMap(1, 10 * 1000, true);
        //doConcurrentSinfoniaHashMap(2, false);
        //doConcurrentSinfoniaHashMap(4, true);
        //doConcurrentSinfoniaHashMap(4, false);
        //doConcurrentSinfoniaHashMap(4, false);
    }

    public void ignore_testFileBasedSinfoniaHashMap() throws Exception {
        useFileMemoryNode = true;
        addressSpace = 60000;
        itemSize = 4096;
        doConcurrentSinfoniaHashMap(2, 1000 * 1000, true);
    }

    public void doConcurrentSinfoniaHashMap(final int numThreads,
            final int numPuts, final boolean putAll) throws Exception {
        long time = System.currentTimeMillis();
        final ApplicationNode appNode = getApplicationNode();
        initializeSinfoniaHashMap();
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            workers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        TransactionManager txManager = new TransactionManager(
                                appNode, new DataItemCache(128));
                        SinfoniaHashMap<Integer, Integer> map = createSinfoniaHashMap(
                                txManager, hashMapHeaderRef);
                        Random random = new Random(Thread.currentThread().hashCode());
                        Map<Integer, Integer> tmp = new HashMap<>();
                        for (int i = 0; i < numPuts / numThreads; i++) {
                            if (i % 1000 == 0) {
                                // put in batches if putAll == true
                                System.out.println(Thread.currentThread().getName() + ": put " + i + " entries");
                                if (!tmp.isEmpty()) {
                                    map.putAll(tmp);
                                    tmp.clear();
                                }
                            }
                            if (putAll) {
                                tmp.put(random.nextInt(), i);
                            } else {
                                map.put(random.nextInt(), i);
                            }
                        }
                        if (!tmp.isEmpty()) {
                            map.putAll(tmp);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        time = System.currentTimeMillis() - time;
        for (Exception e : exceptions) {
            fail(e.toString());
        }
        System.out.println("SinfoniaHashMap(" + numThreads + ", " + putAll + "): " + time);
    }

    public long runRandom(Map<Integer, Integer> map, int numKeys) {
        Random rand = new Random(0);
        long time = System.currentTimeMillis();
        for (int i = 0; i < numKeys; i++) {
            int key = rand.nextInt();
            // System.out.println("map.put(" + key + ", " + i + ")");
            map.put(key, i);
            assertEquals((Integer) i, map.get(key));
        }
        time = System.currentTimeMillis() - time;
        return time;
    }

    private SinfoniaHashMap<Integer, Integer> initializeSinfoniaHashMap()
            throws IOException {
        ApplicationNode appNode = getApplicationNode();
        TransactionManager txManager = new TransactionManager(appNode, new DataItemCache(128));

        final List<ItemReference> itemMgrHeaderRefs = txManager.execute(new Transaction<List<ItemReference>>() {
            @Override
            public List<ItemReference> perform(TransactionContext txContext) {
                List<ItemReference> headerRefs = new ArrayList<>();
                for (int i = 0; i < numMemoryNodes; i++) {
                    headerRefs.add(ItemManagerImpl.initialize(txContext, i, addressSpace));
                }
                return headerRefs;
            }
        });

        hashMapHeaderRef = txManager.execute(new Transaction<ItemReference>() {
            @Override
            public ItemReference perform(TransactionContext txContext) {
                Map<Integer, ItemManager> itemMgrs = new HashMap<>();
                for (ItemReference r : itemMgrHeaderRefs) {
                    itemMgrs.put(r.getMemoryNodeId(), new ItemManagerImpl(txContext, r));
                }
                return new CompositeItemManager(itemMgrs).alloc();
            }
        });

        return createSinfoniaHashMap(txManager, hashMapHeaderRef);
    }

    private SinfoniaHashMap<Integer, Integer> createSinfoniaHashMap(
            TransactionManager txManager, ItemReference headerRef) {
        return new SinfoniaHashMap<>(txManager,
                new ItemManagerFactory() {
                    @Override
                    public ItemManager createItemManager(TransactionContext txContext) {
                        Map<Integer, ItemManager> itemMgrs = new HashMap<>();
                        for (int i = 0; i < numMemoryNodes; i++) {
                            ItemReference headerRef = new ItemReference(i, 0);
                            itemMgrs.put(i, new ItemManagerImpl(txContext, headerRef));
                        }
                        return new CompositeItemManager(itemMgrs);
                    }
                },
                headerRef,
                new BucketReader<Entry<Integer, Integer>>() {
                    @Override
                    public Iterable<Entry<Integer, Integer>> read(
                            ByteBuffer data) {
                        Map<Integer, Integer> entries = new HashMap<>();
                        char num = data.getChar();
                        for (int i = 0; i < num; i++) {
                            entries.put(data.getInt(), data.getInt());
                        }
                        return entries.entrySet();
                    }
                },
                new BucketWriter<Entry<Integer, Integer>>() {
                    @Override
                    public int write(Iterable<Entry<Integer, Integer>> entries,
                                     ByteBuffer data) {
                        char num = 0;
                        data.putChar(num);
                        try {
                            for (Entry<Integer, Integer> e : entries) {
                                data.putInt(e.getKey()).putInt(e.getValue());
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

    private ApplicationNode getApplicationNode() throws IOException {
        if (appNode == null) {
            File testDir = new File("target", "memoryNodes");
            if (testDir.exists()) {
                delete(testDir);
            }
            SimpleMemoryNodeDirectory<MemoryNode> directory = new SimpleMemoryNodeDirectory<>();
            for (int i = 0; i < numMemoryNodes; i++) {
                if (useFileMemoryNode) {
                    File memoryNodeDir = new File(testDir, ""+ i);
                    if (!memoryNodeDir.mkdirs()) {
                        fail("Unable to create memory node directory: " + memoryNodeDir.getAbsolutePath());
                    }
                    directory.addMemoryNode(new FileMemoryNode(i, new File(memoryNodeDir, "data"),
                            addressSpace, itemSize, 1024));

                } else {
                    directory.addMemoryNode(new InMemoryMemoryNode(i, addressSpace, itemSize));
                }
            }
            dir = directory;
            appNode = new SimpleApplicationNode(directory, EXECUTOR);
        }
        return appNode;
    }

    private void delete(File file) {
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            delete(files[i]);
        }
        file.delete();
    }
}
