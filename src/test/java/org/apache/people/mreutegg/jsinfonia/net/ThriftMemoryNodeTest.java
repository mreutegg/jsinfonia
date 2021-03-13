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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeTestBase;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.apache.people.mreutegg.jsinfonia.net.ApplicationNodeClient;
import org.apache.people.mreutegg.jsinfonia.net.ApplicationNodeServer;
import org.apache.people.mreutegg.jsinfonia.net.MemoryNodeClient;
import org.apache.people.mreutegg.jsinfonia.net.MemoryNodeServer;
import org.junit.Test;

public class ThriftMemoryNodeTest extends MemoryNodeTestBase {

    @Test
    public void testSinfoniaThrift() throws Exception {
        int addressSpace = 64 * 1024;
        int numMemoryNodes = 2;
        for (int i = 0; i < 3; i++) {
            testSinfoniaThrift(numMemoryNodes, addressSpace / numMemoryNodes,
                    1024, 1024, numMemoryNodes, true);
            numMemoryNodes *= 2;
        }
    }
    
    @Test
    public void testApplicationNodeThrift() throws Exception {
        testApplicationNodeThrift(4, 16 * 1024, 1024, 1024, 4, true);
    }
    
    private void testSinfoniaThrift(
            final int numMemoryNodes,
            final int addressSpace,
            final int itemSize,
            final int bufferSize,
            final int numThreads,
            final boolean nonBlocking) throws Exception {
        final List<MemoryNode> memoryNodes = new ArrayList<>();
        final List<MemoryNodeServer> servers = new ArrayList<>();
        final SimpleMemoryNodeDirectory<MemoryNodeClient> directory = new SimpleMemoryNodeDirectory<>();
        ExecutorService executor = Executors.newFixedThreadPool(numMemoryNodes);
        try {
            for (int i = 0; i < numMemoryNodes; i++) {
                MemoryNode mn = new InMemoryMemoryNode(i, addressSpace, itemSize);
                memoryNodes.add(mn);
                MemoryNodeServer server = new MemoryNodeServer(mn, 0, nonBlocking);
                server.start();
                servers.add(server);
                directory.addMemoryNode(new MemoryNodeClient("localhost", server.getPort(), numThreads, nonBlocking));
            }
            testSinfonia(directory, addressSpace, itemSize, numThreads);
        } finally {
            Collection<Callable<Void>> closes = new ArrayList<>();
            for (int i = 0; i < directory.getMemoryNodeIds().size(); i++) {
                final MemoryNodeClient client = directory.getMemoryNode(i);
                closes.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        client.close();
                        return null;
                    }
                });
            }
            for (final MemoryNodeServer server : servers) {
                closes.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        server.stop();
                        return null;
                    }
                });
            }
            executor.invokeAll(closes);
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
        }
    }

    private void testApplicationNodeThrift(
            final int numMemoryNodes,
            final int addressSpace,
            final int itemSize,
            final int bufferSize,
            final int numThreads,
            final boolean nonBlocking) throws Exception {
        final SimpleMemoryNodeDirectory<MemoryNode> directory = new SimpleMemoryNodeDirectory<>();
        ExecutorService executor = Executors.newFixedThreadPool(numMemoryNodes);
        ApplicationNodeServer appNodeServer = null;
        try {
            for (int i = 0; i < numMemoryNodes; i++) {
                MemoryNode mn = new InMemoryMemoryNode(i, addressSpace, itemSize);
                directory.addMemoryNode(mn);
            }
            ApplicationNode appNode = new SimpleApplicationNode(directory, executor);
            appNodeServer = new ApplicationNodeServer(appNode, 0, nonBlocking);
            appNodeServer.start();
            ApplicationNodeClient appClient = new ApplicationNodeClient(
                    "localhost", appNodeServer.getPort(), numThreads, nonBlocking);
            testSinfonia(appClient, addressSpace, itemSize, numThreads);
        } finally {
            Collection<Callable<Void>> closes = new ArrayList<>();
            if (appNodeServer != null) {
                final ApplicationNodeServer ans = appNodeServer;
                closes.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ans.stop();
                        return null;
                    }
                });
            }
            for (int i = 0; i < numMemoryNodes; i++) {
                final MemoryNode mn = directory.getMemoryNode(i);
                closes.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (mn instanceof Closeable) {
                            ((Closeable) mn).close();
                        }
                        return null;
                    }
                });
            }
            executor.invokeAll(closes);
            executor.shutdownNow();
        }
    }
}
