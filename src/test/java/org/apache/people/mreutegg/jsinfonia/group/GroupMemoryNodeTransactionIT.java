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
package org.apache.people.mreutegg.jsinfonia.group;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.Vote;
import org.apache.people.mreutegg.jsinfonia.data.DataItem;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.group.MemoryNodeGroupClient;
import org.apache.people.mreutegg.jsinfonia.group.MemoryNodeGroupMember;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.junit.Test;

public class GroupMemoryNodeTransactionIT {

    private static final int NUM_INCREMENTS = 1000;

    @Test
    public void singleMember() throws Exception {
        performTest(1);
    }

    @Test
    public void replicatedMemoryNode() throws Exception {
        performTest(3);
    }

    private void performTest(int numMemoryNodes) throws Exception {
        List<MemoryNode> memoryNodes = new ArrayList<>();
        List<MemoryNodeGroupMember> members = new ArrayList<>();
        for (int i = 0; i < numMemoryNodes; i++) {
            MemoryNode memoryNode = new InMemoryMemoryNode(0, 1024, 1024);
            MemoryNodeGroupMember member = new MemoryNodeGroupMember(memoryNode);
            memoryNodes.add(memoryNode);
            members.add(member);
        }

        MemoryNodeGroupClient client = new MemoryNodeGroupClient(memoryNodes.get(0).getInfo().getId());

        assertEquals(memoryNodes.get(0).getInfo().getAddressSpace(), client.getInfo().getAddressSpace());
        assertEquals(memoryNodes.get(0).getInfo().getId(), client.getInfo().getId());
        assertEquals(memoryNodes.get(0).getInfo().getItemSize(), client.getInfo().getItemSize());

        SimpleMemoryNodeDirectory<MemoryNode> directory = new SimpleMemoryNodeDirectory<>();
        directory.addMemoryNode(client);

        ApplicationNode appNode = new SimpleApplicationNode(directory, Executors.newCachedThreadPool());
        TransactionManager txManager = new TransactionManager(appNode, new DataItemCache(128));
        long time = System.currentTimeMillis();
        for (int i = 0; i < NUM_INCREMENTS; i++) {
            txManager.execute(new Transaction<Void>() {
                @Override
                public Void perform(TransactionContext txContext) {
                    final int v = txContext.read(new ItemReference(0, 0),
                            new DataOperation<Integer>() {
                                @Override
                                public Integer perform(ByteBuffer data) {
                                    return data.getInt();
                                }
                    });
                    // System.out.println("read value: " + v);
                    txContext.write(new ItemReference(0, 0),
                            new DataOperation<Void>() {
                                @Override
                                public Void perform(ByteBuffer data) {
                                    data.putInt(v + 1);
                                    return null;
                                }
                    });
                    return null;
                }
            });
        }
        time = System.currentTimeMillis() - time;
        long txPerSecond = NUM_INCREMENTS * 1000 / time;
        System.out.println(NUM_INCREMENTS + " transactions performed in " + time
                + " ms (" + txPerSecond + " tx/s).");

        Thread.sleep(1000);

        for (MemoryNode memoryNode : memoryNodes) {
            MiniTransaction mt = appNode.createMiniTransaction();
            Item item = new Item(memoryNode.getInfo(), 0);
            mt.addReadItem(item);
            // read directly on memory node
            while (memoryNode.executeAndPrepare(mt, mt.getMemoryNodeIds())
                    .getVote() != Vote.OK) {
                // execute again
            }
            assertEquals(NUM_INCREMENTS, new DataItem(item.getData()).getData().getInt());
        }
        client.close();
        for (MemoryNodeGroupMember member : members) {
            member.close();
        }
    }
}
