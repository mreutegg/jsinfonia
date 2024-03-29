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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.fs.FileMemoryNode;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;


import junit.framework.TestCase;

public abstract class AbstractTransactionTest extends TestCase {

    protected final File testDir = new File(new File("target"), "memoryNodes");
    protected ExecutorService executor;
    protected final List<Runnable> shutdownHooks = new ArrayList<>();
    protected MemoryNodeDirectory<? extends MemoryNode> directory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (testDir.exists()) {
            delete(testDir);
        }
        executor = Executors.newCachedThreadPool();
        shutdownHooks.clear();
        directory = createDirectory();
    }

    @Override
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
        for (Runnable r : shutdownHooks) {
            r.run();
        }
        shutdownHooks.clear();
        if (testDir.exists()) {
            delete(testDir);
        }
        directory = null;
        super.tearDown();
    }

    protected abstract MemoryNodeDirectory<? extends MemoryNode> createDirectory() throws IOException;

    protected TransactionManager createTransactionContext() {
        return new TransactionManager(new SimpleApplicationNode(
                directory, executor), new DataItemCache());
    }

    protected final MemoryNodeDirectory<? extends MemoryNode> createDirectory(
            int numNodes, int addressSpace, int itemSize, int bufferSize) throws IOException {
        SimpleMemoryNodeDirectory<MemoryNode> dir = new SimpleMemoryNodeDirectory<>();
        for (int i = 0; i < numNodes; i++) {
            MemoryNode mn = new InMemoryMemoryNode(i, addressSpace, itemSize);
            dir.addMemoryNode(mn);
        }
        return dir;
    }

    private void delete(File file) {
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            delete(files[i]);
        }
        for (int i = 0; i < 10; i++) {
            if (file.delete()) {
                return;
            } else {
                // file may be memory mapped and can only
                // be deleted when the MappedByteBuffer is
                // garbage collected. Try some GC cycles.
                System.gc();
            }
        }
    }
}
