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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;

/**
 * Base class for simple memory node test.
 */
public abstract class MemoryNodeTestBase {

    protected void testSinfonia(
            final MemoryNodeDirectory<? extends MemoryNode> directory,
            final int addressSpace,
            final int itemSize,
            final int numThreads) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        ApplicationNode appNode = new SimpleApplicationNode(
                directory, executor);
        testSinfonia(appNode, addressSpace, itemSize, numThreads);
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }

    protected void testSinfonia(
            final ApplicationNode appNode,
            final int addressSpace,
            final int itemSize,
            final int numThreads) throws Exception {

        final Map<Integer, MemoryNodeInfo> infos = appNode.getMemoryNodeInfos();
        final int numMemoryNodes = infos.size();
        final AtomicLong successfulWrites = new AtomicLong();
        final AtomicLong failedWrites = new AtomicLong();
        final AtomicLong successfulReads = new AtomicLong();
        final AtomicLong failedReads = new AtomicLong();

        // initialize all items with zero long value
//        for (int i = 0; i < numMemoryNodes; i++) {
//            byte[] data = new byte[itemSize];
//            ByteBuffer dataBuffer = ByteBuffer.wrap(data, 0, data.length);
//            dataBuffer.putLong(0);
//            for (int a = 0; a < addressSpace; a++) {
//                MiniTransaction tx = appNode.createMiniTransaction();
//                tx.addWriteItem(new Item(i, a, 0, data));
//                if (!appNode.executeTransaction(tx)) {
//                    fail("unable to initialize memory nodes with test data");
//                }
//            }
//        }

        // perform transactions with numThreads
        List<Thread> workers = new ArrayList<Thread>();
        for (int i = 0; i < numThreads; i++) {
            workers.add(new Thread(new Runnable() {

                final Random r = new Random();

                @Override
                public void run() {
                    for (int i = 0; i < (4000 / numThreads); i++) {
                        // random read from some memory node
                        MiniTransaction tx = appNode.createMiniTransaction();
                        int memoryNodeId = r.nextInt(numMemoryNodes);
                        MemoryNodeInfo info = infos.get(memoryNodeId);
                        int address = r.nextInt(addressSpace);
                        Item readItem = new Item(info, address);
                        tx.addReadItem(readItem);
                        if (appNode.executeTransaction(tx).isSuccess()) {
                            successfulReads.incrementAndGet();
                            ByteBuffer readBuffer = readItem.getData();
                            long value = readBuffer.getLong();
                            // System.out.println("" + value + "(" + address + ")");
                            Item compareItem = new Item(info, address);
                            ByteBuffer compareBuffer = compareItem.getData();
                            compareBuffer.putLong(value);
                            Item writeItem = new Item(info, address);
                            ByteBuffer writeBuffer = writeItem.getData();
                            writeBuffer.putLong(value + 1);
                            tx = appNode.createMiniTransaction();
                            tx.addCompareItem(compareItem);
                            tx.addWriteItem(writeItem);
                            if (appNode.executeTransaction(tx).isSuccess()) {
                                successfulWrites.incrementAndGet();
                            } else {
                                failedWrites.incrementAndGet();
                            }
                        } else {
                            failedReads.incrementAndGet();
                        }
                    }
                }
            }));
        }

        long time = System.currentTimeMillis();
        for (Thread t : workers) {
            t.start();
        }

        for (Thread t: workers) {
            t.join();
        }
        long numTransactions = successfulReads.longValue() + failedReads.longValue()
                + successfulWrites.longValue() + failedWrites.longValue();

        time = Math.max(System.currentTimeMillis() - time, 1); // avoid division by zero
        System.out.println("" + numTransactions + " transactions performed in " + time + " ms ("
                + (numTransactions * 1000 / time) + " tx/s). ("
                + numMemoryNodes + ", " + addressSpace + ", " + itemSize + ", " + numThreads + ")");

        System.out.println("reads: " + successfulReads + " (successful) / " + failedReads + " (failed)");
        System.out.println("writes: " + successfulWrites + " (successful) / " + failedWrites + " (failed)");
    }
    
    protected void delete(File file) {
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            delete(files[i]);
        }
        for (int i = 0; i < 100; i++) {
            if (file.delete()) {
                return;
            } else {
                // file may be memory mapped and can only
                // be deleted when the MappedByteBuffer is
                // garbage collected. Try some GC cycles.
                System.gc();
            }
        }
        throw new RuntimeException("Cannot delete file: " + file.getAbsolutePath());
    }
}
